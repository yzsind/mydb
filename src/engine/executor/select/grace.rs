//! Disk partitioning and RAII cleanup for grace hash join.
//!
//! When the hash join build side exceeds the memory threshold, hash-split
//! build/probe into P parts each, written to temp files under `<MYDB_DATA_DIR>/tmp/hashjoin-<pid>-<seq>/`; each part then runs an in-memory
//! hash join. Partitions are registered on an RAII guard, so the temp directory is cleaned up on both success and failure.
//!
//! At startup `cleanup_orphan_tmp_dirs` scans `tmp/` and deletes leftover directories (left by a crashed process).

use crate::engine::executor::vec::batch::{Batch, ColumnAccumulator, ColumnVector};
use crate::types::{ColumnType, QueryError, Value};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::plan::{ColMeta, PlanField, PlanRow};

/// Route a precomputed join-key hash at one Grace recursion level.
///
/// A depth-specific XOR by itself is not enough when `partitions` is a power
/// of two: every row in a parent partition has the same low bits, so XOR only
/// renames that partition and recursion never shrinks it. The SplitMix64
/// finalizer folds high bits back into the low bits, giving each level an
/// independent deterministic distribution while equal build/probe keys stay
/// co-located.
#[inline]
pub(super) fn partition_for_hash(hash: u64, depth: u32, partitions: u64) -> u64 {
    debug_assert!(partitions > 0);
    let seed = 0xdead_beef_cafe_babe ^ (u64::from(depth) + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut value = hash ^ seed;
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    (value ^ (value >> 31)) % partitions
}

pub(super) use crate::engine::executor::spill_value::*;

/// Serialise `ColumnType` into the grace-spill side-file. Decimal carries
/// extra `(precision, scale)` bytes so the schema rebuilds losslessly on
/// read; spill files are owned per-query (tmp dir cleaned on close), so
/// no on-disk format compatibility constraint applies.
fn write_col_type<W: Write>(w: &mut W, ct: ColumnType) -> std::io::Result<()> {
    match ct {
        ColumnType::Tiny => w.write_all(&[0]),
        ColumnType::Short => w.write_all(&[1]),
        ColumnType::Long => w.write_all(&[2]),
        ColumnType::LongLong => w.write_all(&[3]),
        ColumnType::Float => w.write_all(&[4]),
        ColumnType::Double => w.write_all(&[5]),
        ColumnType::Decimal { precision, scale } => w.write_all(&[6, precision, scale]),
        ColumnType::Date => w.write_all(&[7]),
        ColumnType::Time => w.write_all(&[8]),
        ColumnType::DateTime => w.write_all(&[9]),
        ColumnType::Timestamp => w.write_all(&[10]),
        ColumnType::Blob => w.write_all(&[11]),
        ColumnType::Json => w.write_all(&[12]),
        ColumnType::VarString => w.write_all(&[13]),
        ColumnType::Geometry => w.write_all(&[14]),
        ColumnType::Vector => w.write_all(&[15]),
        // New tags for MEDIUMINT/YEAR — appended at the end of the
        // existing list so tags 0..=15 keep their meaning for tmp-spill
        // files produced by older builds. Tmp files are per-query and
        // cleaned on close, so there is no on-disk compatibility burden
        // beyond intra-process churn.
        ColumnType::MediumInt => w.write_all(&[16]),
        ColumnType::Year => w.write_all(&[17]),
        ColumnType::Bit { bits } => w.write_all(&[18, bits]),
    }
}

fn read_col_type<R: std::io::Read>(r: &mut R) -> Result<ColumnType, QueryError> {
    let mut tag = [0u8; 1];
    r.read_exact(&mut tag).map_err(io_err)?;
    Ok(match tag[0] {
        0 => ColumnType::Tiny,
        1 => ColumnType::Short,
        2 => ColumnType::Long,
        3 => ColumnType::LongLong,
        4 => ColumnType::Float,
        5 => ColumnType::Double,
        6 => {
            let mut buf = [0u8; 2];
            r.read_exact(&mut buf).map_err(io_err)?;
            ColumnType::Decimal {
                precision: buf[0],
                scale: buf[1],
            }
        }
        7 => ColumnType::Date,
        8 => ColumnType::Time,
        9 => ColumnType::DateTime,
        10 => ColumnType::Timestamp,
        11 => ColumnType::Blob,
        12 => ColumnType::Json,
        13 => ColumnType::VarString,
        14 => ColumnType::Geometry,
        15 => ColumnType::Vector,
        16 => ColumnType::MediumInt,
        17 => ColumnType::Year,
        18 => {
            let mut buf = [0u8; 1];
            r.read_exact(&mut buf).map_err(io_err)?;
            ColumnType::Bit { bits: buf[0] }
        }
        other => {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                format!("invalid column type tag in spill file: {}", other),
            ))
        }
    })
}

fn write_opt_str<W: Write>(w: &mut W, s: Option<&str>) -> std::io::Result<()> {
    match s {
        None => write_u32(w, u32::MAX),
        Some(s) => write_str(w, s),
    }
}
fn write_opt_u32<W: Write>(w: &mut W, value: Option<u32>) -> std::io::Result<()> {
    write_u32(w, value.unwrap_or(u32::MAX))
}
fn write_opt_u16<W: Write>(w: &mut W, value: Option<u16>) -> std::io::Result<()> {
    w.write_all(&value.unwrap_or(u16::MAX).to_le_bytes())
}
fn write_opt_u8<W: Write>(w: &mut W, value: Option<u8>) -> std::io::Result<()> {
    w.write_all(&[value.unwrap_or(u8::MAX)])
}

fn read_opt_str<R: Read>(r: &mut R) -> std::io::Result<Option<String>> {
    let len = read_u32(r)?;
    if len == u32::MAX {
        return Ok(None);
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)?;
    Ok(Some(String::from_utf8(buf).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?))
}
fn read_opt_u32<R: Read>(r: &mut R) -> std::io::Result<Option<u32>> {
    let value = read_u32(r)?;
    Ok((value != u32::MAX).then_some(value))
}
fn read_opt_u16<R: Read>(r: &mut R) -> std::io::Result<Option<u16>> {
    let mut bytes = [0u8; 2];
    r.read_exact(&mut bytes)?;
    let value = u16::from_le_bytes(bytes);
    Ok((value != u16::MAX).then_some(value))
}
fn read_opt_u8<R: Read>(r: &mut R) -> std::io::Result<Option<u8>> {
    let mut bytes = [0u8; 1];
    r.read_exact(&mut bytes)?;
    Ok((bytes[0] != u8::MAX).then_some(bytes[0]))
}

/// Inverse of [`write_value_payload`]: reads the payload given an
/// externally-known tag. Identical to [`read_value_with_first_tag`]
/// but consumes no input for tag bytes — the caller has already
/// decided the tag via the schema header.
pub(super) fn read_value_payload<R: Read>(r: &mut R, tag: u8) -> Result<Value, QueryError> {
    read_value_with_first_tag(r, tag)
}

/// Serialize one typed batch cell in the exact legacy `Value` spill format
/// without materialising an owned `Value`.
fn write_column_value<W: Write>(
    w: &mut W,
    column: &ColumnVector,
    row: usize,
) -> std::io::Result<()> {
    if column.is_null(row) {
        return w.write_all(&[VALUE_NULL]);
    }
    match column {
        ColumnVector::Null { .. } => w.write_all(&[VALUE_NULL]),
        ColumnVector::Bool { values, .. } => {
            w.write_all(&[VALUE_INT])?;
            w.write_all(&(values.get(row) as i64).to_le_bytes())
        }
        ColumnVector::I64 { data, .. } => {
            w.write_all(&[VALUE_INT])?;
            w.write_all(&data[row].to_le_bytes())
        }
        ColumnVector::U64 { data, .. } => {
            w.write_all(&[VALUE_UINT])?;
            w.write_all(&data[row].to_le_bytes())
        }
        ColumnVector::F64 { data, .. } => {
            w.write_all(&[VALUE_DOUBLE])?;
            w.write_all(&data[row].to_bits().to_le_bytes())
        }
        ColumnVector::Date32 { data, .. } => {
            w.write_all(&[VALUE_DATE])?;
            w.write_all(&data[row].to_le_bytes())
        }
        ColumnVector::Time64 { data, .. } => {
            w.write_all(&[VALUE_TIME])?;
            w.write_all(&data[row].to_le_bytes())
        }
        ColumnVector::DateTime64 { data, .. } => {
            w.write_all(&[VALUE_DATETIME])?;
            w.write_all(&data[row].to_le_bytes())
        }
        ColumnVector::Timestamp64 { data, .. } => {
            w.write_all(&[VALUE_TIMESTAMP])?;
            w.write_all(&data[row].to_le_bytes())
        }
        ColumnVector::Decimal32 { data, scale, .. } => {
            w.write_all(&[VALUE_DECIMAL_COMPACT])?;
            w.write_all(&(data[row] as i64).to_le_bytes())?;
            w.write_all(&[*scale])
        }
        ColumnVector::Decimal64 { data, scale, .. } => {
            w.write_all(&[VALUE_DECIMAL_COMPACT])?;
            w.write_all(&data[row].to_le_bytes())?;
            w.write_all(&[*scale])
        }
        ColumnVector::Decimal128 { data, scale, .. } => {
            if let Ok(small) = i64::try_from(data[row]) {
                w.write_all(&[VALUE_DECIMAL_COMPACT])?;
                w.write_all(&small.to_le_bytes())?;
            } else {
                w.write_all(&[VALUE_DECIMAL128])?;
                w.write_all(&data[row].to_le_bytes())?;
            }
            w.write_all(&[*scale])
        }
        ColumnVector::DecimalWide { data, .. } => {
            w.write_all(&[VALUE_DECIMAL_WIDE])?;
            let bytes = data[row].magnitude.to_signed_bytes_le();
            write_bytes(w, &bytes)?;
            w.write_all(&[data[row].scale])
        }
        ColumnVector::Utf8 { offsets, data, .. } => {
            let bytes = &data[offsets[row] as usize..offsets[row + 1] as usize];
            if std::str::from_utf8(bytes).is_ok() {
                w.write_all(&[VALUE_STRING])?;
                write_bytes(w, bytes)
            } else {
                write_value(w, &column.value_at(row))
            }
        }
        ColumnVector::Bytes { offsets, data, .. }
        | ColumnVector::Json { offsets, data, .. }
        | ColumnVector::Geometry { offsets, data, .. }
        | ColumnVector::Vector { offsets, data, .. } => {
            let tag = match column {
                ColumnVector::Bytes { .. } => VALUE_BYTES,
                ColumnVector::Json { .. } => VALUE_JSON,
                ColumnVector::Geometry { .. } => VALUE_GEOMETRY,
                ColumnVector::Vector { .. } => VALUE_VECTOR,
                _ => unreachable!(),
            };
            w.write_all(&[tag])?;
            write_bytes(w, &data[offsets[row] as usize..offsets[row + 1] as usize])
        }
        ColumnVector::Generic { values } => write_value(w, &values[row]),
    }
}

pub(super) fn io_err(e: std::io::Error) -> QueryError {
    if let Some(error) = crate::engine::executor::spill_quota::query_error_from_io(&e) {
        return error;
    }
    crate::storage::error(
        crate::error::catalog::ER_UNKNOWN_ERROR,
        format!("hash join spill I/O: {}", e),
    )
}

/// A partition writer: append mode, tracks the byte count.
pub(super) struct PartitionWriter {
    writer: crate::engine::executor::spill_quota::TrackedSpillWriter,
    schema_written: bool,
}

impl PartitionWriter {
    /// Create a partition writer whose appended rows are charged against
    /// the temp-disk quota via `charger`.
    pub(super) fn create_charged(
        path: &Path,
        charger: crate::engine::executor::spill_quota::SpillCharger,
    ) -> Result<Self, QueryError> {
        Self::create_charged_with_writer_count(path, charger, 1)
    }

    pub(super) fn create_charged_with_writer_count(
        path: &Path,
        charger: crate::engine::executor::spill_quota::SpillCharger,
        writer_count: usize,
    ) -> Result<Self, QueryError> {
        let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(writer_count);
        Ok(Self {
            writer: crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
                path, charger, capacity,
            )?,
            schema_written: false,
        })
    }

    fn open_append_charged_with_writer_count(
        path: &Path,
        charger: crate::engine::executor::spill_quota::SpillCharger,
        writer_count: usize,
    ) -> Result<Self, QueryError> {
        let schema_written = path.metadata().map(|meta| meta.len() > 0).unwrap_or(false);
        let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(writer_count);
        Ok(Self {
            writer: crate::engine::executor::spill_quota::TrackedSpillWriter::open_append(
                path, charger, capacity,
            )?,
            schema_written,
        })
    }

    pub(super) fn append_row(&mut self, row: &PlanRow) -> Result<(), QueryError> {
        // The first row writes the full schema (metadata of every PlanField); later
        // rows write values only, greatly cutting repeated metadata storage/IO overhead.
        if !self.schema_written {
            write_u32(&mut self.writer, row.fields.len() as u32).map_err(io_err)?;
            for f in &row.fields {
                write_str(&mut self.writer, &f.table).map_err(io_err)?;
                write_opt_str(&mut self.writer, f.alias.as_deref()).map_err(io_err)?;
                write_str(&mut self.writer, &f.column).map_err(io_err)?;
                write_col_type(&mut self.writer, f.column_type).map_err(io_err)?;
                self.writer
                    .write_all(&[u8::from(f.invisible)])
                    .map_err(io_err)?;
                write_str(&mut self.writer, &f.source.schema).map_err(io_err)?;
                write_str(&mut self.writer, &f.source.table).map_err(io_err)?;
                write_str(&mut self.writer, &f.source.org_table).map_err(io_err)?;
                write_str(&mut self.writer, &f.source.org_name).map_err(io_err)?;
                self.writer
                    .write_all(&f.source.flags.to_le_bytes())
                    .map_err(io_err)?;
                write_opt_u32(&mut self.writer, f.source.length).map_err(io_err)?;
                write_opt_u8(&mut self.writer, f.source.numeric_precision).map_err(io_err)?;
                write_opt_u8(&mut self.writer, f.source.decimals).map_err(io_err)?;
                write_opt_u16(&mut self.writer, f.source.collation_id).map_err(io_err)?;
                write_opt_u8(&mut self.writer, f.source.collation_coercibility).map_err(io_err)?;
                write_opt_u8(&mut self.writer, f.source.wire_type).map_err(io_err)?;
                self.writer
                    .write_all(&[u8::from(f.source.dynamic_parameter)])
                    .map_err(io_err)?;
            }
            self.schema_written = true;
        }
        for f in &row.fields {
            write_value(&mut self.writer, &f.value).map_err(io_err)?;
        }
        // PlanRows produced by HashAggregate carry finalized aggregate values
        // outside `fields`. External Sort may spill those rows before the
        // final projection/ORDER BY expression is evaluated, so dropping this
        // sidecar turns valid aggregate references into error 1111 on replay.
        // Temp files are process-local: extend the row format with a compact
        // metadata trailer (group-presence flag + aggregate key/value map).
        let mut flags = 0u8;
        if row.group.is_some() {
            flags |= 0x01;
        }
        if row.agg_values.is_some() {
            flags |= 0x02;
        }
        self.writer.write_all(&[flags]).map_err(io_err)?;
        if let Some(values) = &row.agg_values {
            write_u32(&mut self.writer, values.len() as u32).map_err(io_err)?;
            for (key, value) in values.iter() {
                self.writer.write_all(&key.to_le_bytes()).map_err(io_err)?;
                write_value(&mut self.writer, value).map_err(io_err)?;
            }
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<u64, QueryError> {
        self.writer.finish()
    }
}

/// Lazily opens partition files and evicts the least-recently-used writer.
/// Build and probe each get a pool of at most 16 writers, keeping the Grace
/// coordinator's combined file-descriptor fan-out at 32 even when logical
/// fanout is 256.
pub(super) struct LazyPartitionWriterPool {
    paths: Vec<PathBuf>,
    charger: crate::engine::executor::spill_quota::SpillCharger,
    slots: Vec<Option<(PartitionWriter, u64)>>,
    max_open: usize,
    epoch: u64,
    bytes: u64,
}

impl LazyPartitionWriterPool {
    pub(super) fn new(
        dir: &Path,
        prefix: &str,
        partitions: usize,
        charger: crate::engine::executor::spill_quota::SpillCharger,
        max_open: usize,
    ) -> Self {
        Self {
            paths: (0..partitions)
                .map(|idx| dir.join(format!("{prefix}-{idx}")))
                .collect(),
            charger,
            slots: (0..partitions).map(|_| None).collect(),
            max_open: max_open.max(1).min(partitions.max(1)),
            epoch: 0,
            bytes: 0,
        }
    }

    pub(super) fn append_row(&mut self, partition: usize, row: &PlanRow) -> Result<(), QueryError> {
        if partition >= self.slots.len() {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "hash join spill partition index out of bounds",
            ));
        }
        self.epoch = self.epoch.wrapping_add(1);
        if self.slots[partition].is_none() {
            let open = self.slots.iter().filter(|slot| slot.is_some()).count();
            if open >= self.max_open {
                let evict = self
                    .slots
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, slot)| slot.as_ref().map(|(_, epoch)| (idx, *epoch)))
                    .min_by_key(|(_, epoch)| *epoch)
                    .map(|(idx, _)| idx)
                    .expect("open writer count is non-zero");
                if let Some((writer, _)) = self.slots[evict].take() {
                    self.bytes = self.bytes.saturating_add(writer.finish()?);
                }
            }
            let writer = PartitionWriter::open_append_charged_with_writer_count(
                &self.paths[partition],
                self.charger.clone(),
                self.max_open.saturating_mul(2),
            )?;
            self.slots[partition] = Some((writer, self.epoch));
        }
        let (writer, used) = self.slots[partition].as_mut().expect("writer was opened");
        *used = self.epoch;
        writer.append_row(row)
    }

    pub(super) fn finish(mut self) -> Result<u64, QueryError> {
        for slot in &mut self.slots {
            if let Some((writer, _)) = slot.take() {
                self.bytes = self.bytes.saturating_add(writer.finish()?);
            }
        }
        Ok(self.bytes)
    }
}

/// Read one partition file: parse the schema header first, then decode rows in
/// a loop. The returned Vec<PlanRow> is equivalent to the pre-spill input.
#[cfg(test)]
pub(super) fn read_partition(
    path: &Path,
    charger: crate::engine::executor::spill_quota::SpillCharger,
) -> Result<Vec<PlanRow>, QueryError> {
    let mut reader = match PartitionReader::open(path, charger, 1)? {
        Some(reader) => reader,
        None => return Ok(Vec::new()),
    };
    let mut rows = Vec::new();
    while let Some(row) = reader.next_row()? {
        rows.push(row);
    }
    Ok(rows)
}

/// Streaming reader for spill partitions. Row hash join applies its budget
/// to each decoded row before retaining a partition; external sort merges
/// runs with only one decoded row per run.
pub(super) struct PartitionReader {
    reader: crate::engine::executor::spill_quota::TrackedSpillReader,
    // Metadata is immutable for the whole partition. Intern it once while
    // reading the header; `next_row` then copies one pointer per field instead
    // of cloning Strings and repeating the TLS interner lookup for every
    // spilled cell.
    schema: Vec<super::plan::ColMetaRef>,
    _schema_charge: crate::engine::executor::mem_budget::ScopedCharge,
    decoded: crate::engine::executor::mem_budget::ScopedCharge,
}

impl PartitionReader {
    pub(super) fn open(
        path: &Path,
        charger: crate::engine::executor::spill_quota::SpillCharger,
        reader_count: usize,
    ) -> Result<Option<Self>, QueryError> {
        if charger.absent_partition(path)? {
            return Ok(None);
        }
        let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(reader_count);
        let mut reader = crate::engine::executor::spill_quota::TrackedSpillReader::open(
            path, charger, capacity,
        )?;
        // The file may be empty (no rows ever written): try to read the schema's first field length; on EOF return empty.
        let Some(field_count) = read_optional_u32(&mut reader).map_err(io_err)? else {
            return Ok(None);
        };
        let field_count = field_count as usize;
        if field_count as u64 > reader.remaining_bytes() {
            return Err(
                crate::error::catalog::ER_UNKNOWN_ERROR.error("invalid spill schema field count")
            );
        }
        let mut schema_charge = reader.memory_charge();
        schema_charge.add_force(
            field_count.saturating_mul(std::mem::size_of::<super::plan::ColMetaRef>()),
        )?;
        let mut schema = crate::engine::executor::mem_budget::try_vec_with_capacity(
            field_count,
            "spill row schema",
        )?;
        for _ in 0..field_count {
            let table = read_str(&mut reader).map_err(io_err)?;
            let alias = read_opt_str(&mut reader).map_err(io_err)?;
            let column = read_str(&mut reader).map_err(io_err)?;
            let column_type = read_col_type(&mut reader)?;
            let mut invisible = [0u8; 1];
            reader.read_exact(&mut invisible).map_err(io_err)?;
            let source_schema = read_str(&mut reader).map_err(io_err)?;
            let source_table = read_str(&mut reader).map_err(io_err)?;
            let source_org_table = read_str(&mut reader).map_err(io_err)?;
            let source_org_name = read_str(&mut reader).map_err(io_err)?;
            let mut flags = [0u8; 2];
            reader.read_exact(&mut flags).map_err(io_err)?;
            let length = read_opt_u32(&mut reader).map_err(io_err)?;
            let numeric_precision = read_opt_u8(&mut reader).map_err(io_err)?;
            let decimals = read_opt_u8(&mut reader).map_err(io_err)?;
            let collation_id = read_opt_u16(&mut reader).map_err(io_err)?;
            let collation_coercibility = read_opt_u8(&mut reader).map_err(io_err)?;
            let wire_type = read_opt_u8(&mut reader).map_err(io_err)?;
            let mut dynamic_parameter = [0u8; 1];
            reader.read_exact(&mut dynamic_parameter).map_err(io_err)?;
            schema.push(crate::engine::executor::select::plan::intern_col_meta(
                ColMeta {
                    table,
                    alias,
                    column,
                    column_type,
                    source: crate::types::ColumnSource {
                        dynamic_parameter: dynamic_parameter[0] != 0,
                        schema: source_schema,
                        table: source_table,
                        org_table: source_org_table,
                        org_name: source_org_name,
                        flags: u16::from_le_bytes(flags),
                        length,
                        numeric_precision,
                        decimals,
                        collation_id,
                        collation_coercibility,
                        wire_type,
                    },
                    invisible: invisible[0] != 0,
                },
            ));
        }
        let decoded = reader.memory_charge();
        Ok(Some(Self {
            reader,
            schema,
            _schema_charge: schema_charge,
            decoded,
        }))
    }

    pub(super) fn next_row(&mut self) -> Result<Option<PlanRow>, QueryError> {
        self.decoded = self.reader.memory_charge();
        let mut peek = [0u8; 1];
        match self.reader.read(&mut peek) {
            Ok(0) => return Ok(None),
            Ok(_) => {}
            Err(e) => return Err(io_err(e)),
        }
        self.decoded.add_force(
            self.schema
                .len()
                .saturating_mul(std::mem::size_of::<PlanField>()),
        )?;
        let mut row_fields = crate::engine::executor::mem_budget::try_vec_with_capacity(
            self.schema.len(),
            "spill row fields",
        )?;
        for (i, meta) in self.schema.iter().enumerate() {
            let value = if i == 0 {
                crate::engine::executor::spill_value::read_payload_accounted(
                    &mut self.reader,
                    peek[0],
                    false,
                    io_err,
                    Some(&mut self.decoded),
                )?
            } else {
                read_value_accounted(&mut self.reader, &mut self.decoded)?
            };
            row_fields.push(PlanField {
                meta: meta.clone(),
                value,
            });
        }
        let mut flags = [0u8; 1];
        if self.schema.is_empty() {
            flags = peek;
        } else {
            self.reader.read_exact(&mut flags).map_err(io_err)?;
        }
        let agg_values = if flags[0] & 0x02 != 0 {
            let count = read_u32(&mut self.reader).map_err(io_err)? as usize;
            if count as u64 > self.reader.remaining_bytes() / 9 {
                return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                    .error("invalid spill aggregate sidecar count"));
            }
            self.decoded
                .add_force(count.saturating_mul(std::mem::size_of::<Value>() + 64))?;
            let mut values = std::collections::HashMap::new();
            values.try_reserve(count).map_err(|_| {
                crate::engine::executor::mem_budget::allocation_error("spill aggregate sidecar")
            })?;
            for _ in 0..count {
                let mut key = [0u8; 8];
                self.reader.read_exact(&mut key).map_err(io_err)?;
                values.insert(
                    u64::from_le_bytes(key),
                    read_value_accounted(&mut self.reader, &mut self.decoded)?,
                );
            }
            Some(std::sync::Arc::new(values))
        } else {
            None
        };
        Ok(Some(PlanRow {
            fields: row_fields,
            group: (flags[0] & 0x01 != 0).then(Vec::new),
            agg_values,
            schema: PlanRow::empty_schema(),
        }))
    }

    pub(super) fn bytes_read(&self) -> u64 {
        self.reader.bytes_read()
    }
}

pub(super) fn read_value_with_first_tag<R: Read>(r: &mut R, tag: u8) -> Result<Value, QueryError> {
    crate::engine::executor::spill_value::read_payload(r, tag, false, io_err)
}

fn read_value_accounted<R: Read>(
    r: &mut R,
    charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
) -> Result<Value, QueryError> {
    let mut tag = [0];
    r.read_exact(&mut tag).map_err(io_err)?;
    crate::engine::executor::spill_value::read_payload_accounted(
        r,
        tag[0],
        false,
        io_err,
        Some(charge),
    )
}

/// Approximate serialized size of a `Vec<Value>` row (1 tag byte + the
/// `write_value` payload), used to charge the temp-disk quota for CTE
/// spill files.
#[allow(dead_code)]
pub(crate) fn estimate_value_row_bytes(row: &[Value]) -> u64 {
    let mut bytes: u64 = 0;
    for v in row {
        bytes = bytes.saturating_add(1); // tag
        bytes = bytes.saturating_add(match v {
            Value::Null => 0,
            Value::Int(_) | Value::UInt(_) | Value::Double(_) => 8,
            Value::Float(_) => 4,
            Value::String(s) | Value::Decimal(s) => 4 + s.len() as u64,
            Value::SmallString(b) => 4 + b.len() as u64,
            Value::Bytes(b) | Value::Json(b) | Value::Geometry(b) | Value::Vector(b) => {
                4 + b.len() as u64
            }
            Value::DecimalCompact(..) => 9,
            Value::Decimal128(..) => 17,
            Value::DecimalWide(_) => 5 + v.decimal_heap_bytes() as u64,
            Value::Date(_) => 4,
            Value::Time(_) | Value::DateTime(_) | Value::Timestamp(_) => 8,
            Value::Year(_) => 1,
        });
    }
    bytes
}

/// P0-4 — spill writer for `Vec<Value>` rows (CTE / derived-table
/// materialization). Each row is `ncols` tagged values (via
/// [`write_value`]); a one-time `u32` header records `ncols`. Every row
/// is charged against the temp-disk quota before it is written.
pub(crate) struct ValueRowWriter {
    writer: crate::engine::executor::spill_quota::TrackedSpillWriter,
    ncols: usize,
    header_written: bool,
}

/// Coalesce the many small writes produced by row/value encoding before they
/// reach `TrackedSpillWriter`/`BufWriter`.
///
/// Grace partitioning serialises one typed value at a time.  A typical fact
/// row therefore used to enter the generic `Write::write_all` stack several
/// times even though the final bytes immediately landed in the same spill
/// buffer.  Keep one bounded, stack-owned staging block per append call and
/// forward complete row-sized chunks instead.  Oversized strings/JSON values
/// are written directly after draining the block, so this never creates an
/// ungoverned heap allocation and retains the existing spill-buffer
/// reservation/lifetime.
struct StagedRowWriter<'a, W: Write, const N: usize> {
    inner: &'a mut W,
    buffer: [u8; N],
    len: usize,
}

impl<'a, W: Write, const N: usize> StagedRowWriter<'a, W, N> {
    fn new(inner: &'a mut W) -> Self {
        Self {
            inner,
            buffer: [0; N],
            len: 0,
        }
    }

    fn drain_buffer(&mut self) -> std::io::Result<()> {
        if self.len > 0 {
            self.inner.write_all(&self.buffer[..self.len])?;
            self.len = 0;
        }
        Ok(())
    }

    fn finish_row(mut self) -> std::io::Result<()> {
        self.drain_buffer()
    }
}

impl<W: Write, const N: usize> Write for StagedRowWriter<'_, W, N> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() >= N {
            self.drain_buffer()?;
            self.inner.write_all(bytes)?;
            return Ok(bytes.len());
        }
        if self.len + bytes.len() > N {
            self.drain_buffer()?;
        }
        self.buffer[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.drain_buffer()?;
        self.inner.flush()
    }
}

const SPILL_ROW_STAGING_BYTES: usize = 1024;

impl ValueRowWriter {
    pub(crate) fn create(
        path: &Path,
        ncols: usize,
        charger: crate::engine::executor::spill_quota::SpillCharger,
    ) -> Result<Self, QueryError> {
        Self::create_with_writer_count(path, ncols, charger, 1)
    }

    pub(crate) fn create_with_writer_count(
        path: &Path,
        ncols: usize,
        charger: crate::engine::executor::spill_quota::SpillCharger,
        writer_count: usize,
    ) -> Result<Self, QueryError> {
        let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(writer_count);
        Ok(Self {
            writer: crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
                path, charger, capacity,
            )?,
            ncols,
            header_written: false,
        })
    }

    fn open_append_with_writer_count(
        path: &Path,
        ncols: usize,
        charger: crate::engine::executor::spill_quota::SpillCharger,
        writer_count: usize,
    ) -> Result<Self, QueryError> {
        let header_written = path.metadata().map(|meta| meta.len() > 0).unwrap_or(false);
        let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(writer_count);
        Ok(Self {
            writer: crate::engine::executor::spill_quota::TrackedSpillWriter::open_append(
                path, charger, capacity,
            )?,
            ncols,
            header_written,
        })
    }

    pub(crate) fn append(&mut self, row: &[Value]) -> Result<(), QueryError> {
        if row.len() != self.ncols {
            return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                .error("spill row width does not match its schema"));
        }
        let mut staged = StagedRowWriter::<_, SPILL_ROW_STAGING_BYTES>::new(&mut self.writer);
        if !self.header_written {
            write_u32(&mut staged, self.ncols as u32).map_err(io_err)?;
            self.header_written = true;
        }
        if self.ncols == 0 {
            staged.write_all(&[VALUE_NULL]).map_err(io_err)?;
        }
        for v in row {
            write_value(&mut staged, v).map_err(io_err)?;
        }
        staged.finish_row().map_err(io_err)?;
        Ok(())
    }

    pub(crate) fn append_batch_row(&mut self, batch: &Batch, row: usize) -> Result<(), QueryError> {
        if batch.columns().len() != self.ncols {
            return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                .error("spill batch row width does not match its schema"));
        }
        let mut staged = StagedRowWriter::<_, SPILL_ROW_STAGING_BYTES>::new(&mut self.writer);
        if !self.header_written {
            write_u32(&mut staged, self.ncols as u32).map_err(io_err)?;
            self.header_written = true;
        }
        debug_assert_eq!(batch.columns().len(), self.ncols);
        if self.ncols == 0 {
            staged.write_all(&[VALUE_NULL]).map_err(io_err)?;
        }
        for column in batch.columns() {
            write_column_value(&mut staged, column, row).map_err(io_err)?;
        }
        staged.finish_row().map_err(io_err)?;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<u64, QueryError> {
        self.writer.finish()
    }
}

pub(crate) struct LazyValueRowWriterPool {
    paths: Vec<PathBuf>,
    charger: crate::engine::executor::spill_quota::SpillCharger,
    slots: Vec<Option<(ValueRowWriter, u64)>>,
    ncols: usize,
    max_open: usize,
    buffer_writer_count: usize,
    epoch: u64,
    bytes: u64,
    opens: u64,
}

impl LazyValueRowWriterPool {
    pub(crate) fn new(
        dir: &Path,
        prefix: &str,
        partitions: usize,
        ncols: usize,
        charger: crate::engine::executor::spill_quota::SpillCharger,
        max_open: usize,
    ) -> Self {
        Self::new_with_concurrent_pools(dir, prefix, partitions, ncols, charger, max_open, 2)
    }

    /// Create a writer pool with an explicit count of simultaneously-live
    /// sibling pools. Grace build/probe partitioning is phase-serial, so each
    /// side can use `concurrent_pools=1` once the completed side is closed.
    pub(crate) fn new_with_concurrent_pools(
        dir: &Path,
        prefix: &str,
        partitions: usize,
        ncols: usize,
        charger: crate::engine::executor::spill_quota::SpillCharger,
        max_open: usize,
        concurrent_pools: usize,
    ) -> Self {
        let max_open = max_open.max(1).min(partitions.max(1));
        Self {
            paths: (0..partitions)
                .map(|idx| dir.join(format!("{prefix}-{idx}")))
                .collect(),
            charger,
            slots: (0..partitions).map(|_| None).collect(),
            ncols,
            max_open,
            buffer_writer_count: max_open.saturating_mul(concurrent_pools.max(1)),
            epoch: 0,
            bytes: 0,
            opens: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn opens(&self) -> u64 {
        self.opens
    }

    pub(crate) fn append(&mut self, partition: usize, row: &[Value]) -> Result<(), QueryError> {
        self.epoch = self.epoch.wrapping_add(1);
        if self.slots[partition].is_none() {
            if self.slots.iter().filter(|slot| slot.is_some()).count() >= self.max_open {
                let evict = self
                    .slots
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, slot)| slot.as_ref().map(|(_, epoch)| (idx, *epoch)))
                    .min_by_key(|(_, epoch)| *epoch)
                    .map(|(idx, _)| idx)
                    .expect("open writer count is non-zero");
                if let Some((writer, _)) = self.slots[evict].take() {
                    self.bytes = self.bytes.saturating_add(writer.finish()?);
                }
            }
            let writer = ValueRowWriter::open_append_with_writer_count(
                &self.paths[partition],
                self.ncols,
                self.charger.clone(),
                self.buffer_writer_count,
            )?;
            self.opens = self.opens.saturating_add(1);
            self.slots[partition] = Some((writer, self.epoch));
        }
        let (writer, used) = self.slots[partition].as_mut().expect("writer was opened");
        *used = self.epoch;
        writer.append(row)
    }

    pub(crate) fn append_batch_row(
        &mut self,
        partition: usize,
        batch: &Batch,
        row: usize,
    ) -> Result<(), QueryError> {
        self.epoch = self.epoch.wrapping_add(1);
        if self.slots[partition].is_none() {
            if self.slots.iter().filter(|slot| slot.is_some()).count() >= self.max_open {
                let evict = self
                    .slots
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, slot)| slot.as_ref().map(|(_, epoch)| (idx, *epoch)))
                    .min_by_key(|(_, epoch)| *epoch)
                    .map(|(idx, _)| idx)
                    .expect("open writer count is non-zero");
                if let Some((writer, _)) = self.slots[evict].take() {
                    self.bytes = self.bytes.saturating_add(writer.finish()?);
                }
            }
            let writer = ValueRowWriter::open_append_with_writer_count(
                &self.paths[partition],
                self.ncols,
                self.charger.clone(),
                self.buffer_writer_count,
            )?;
            self.opens = self.opens.saturating_add(1);
            self.slots[partition] = Some((writer, self.epoch));
        }
        let (writer, used) = self.slots[partition].as_mut().expect("writer was opened");
        *used = self.epoch;
        writer.append_batch_row(batch, row)
    }

    pub(crate) fn finish(mut self) -> Result<u64, QueryError> {
        for slot in &mut self.slots {
            if let Some((writer, _)) = slot.take() {
                self.bytes = self.bytes.saturating_add(writer.finish()?);
            }
        }
        Ok(self.bytes)
    }
}

/// Streaming reader counterpart of [`ValueRowWriter`]. Yields one
/// `Vec<Value>` per `next`, so a spilled CTE can be re-scanned without
/// pulling the whole result back into memory at once.
pub(crate) struct ValueRowReader {
    reader: crate::engine::executor::spill_quota::TrackedSpillReader,
    ncols: usize,
    decoded: crate::engine::executor::mem_budget::ScopedCharge,
    batch_row_cap: usize,
    batch_fixed_bytes: usize,
}

impl ValueRowReader {
    pub(crate) fn column_count(&self) -> usize {
        self.ncols
    }

    pub(crate) fn open(
        path: &Path,
        charger: crate::engine::executor::spill_quota::SpillCharger,
    ) -> Result<Option<Self>, QueryError> {
        let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(1);
        Self::open_with_capacity(path, charger, capacity)
    }

    pub(crate) fn open_with_capacity(
        path: &Path,
        charger: crate::engine::executor::spill_quota::SpillCharger,
        capacity: usize,
    ) -> Result<Option<Self>, QueryError> {
        if charger.absent_partition(path)? {
            return Ok(None);
        }
        let mut reader = crate::engine::executor::spill_quota::TrackedSpillReader::open(
            path, charger, capacity,
        )?;
        let Some(ncols) = read_optional_u32(&mut reader).map_err(io_err)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            decoded: reader.memory_charge(),
            batch_row_cap: 0,
            batch_fixed_bytes: 0,
            reader,
            ncols: ncols as usize,
        }))
    }

    pub(crate) fn next(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        self.decoded = self.reader.memory_charge();
        let mut peek = [0u8; 1];
        match self.reader.read(&mut peek) {
            Ok(0) => return Ok(None),
            Ok(_) => {}
            Err(e) => return Err(io_err(e)),
        }
        self.decoded
            .add_force(self.ncols.saturating_mul(std::mem::size_of::<Value>()))?;
        let mut row = crate::engine::executor::mem_budget::try_vec_with_capacity(
            self.ncols,
            "spill value row",
        )?;
        for i in 0..self.ncols {
            let v = if i == 0 {
                crate::engine::executor::spill_value::read_payload_accounted(
                    &mut self.reader,
                    peek[0],
                    false,
                    io_err,
                    Some(&mut self.decoded),
                )?
            } else {
                read_value_accounted(&mut self.reader, &mut self.decoded)?
            };
            row.push(v);
        }
        Ok(Some(row))
    }

    pub(crate) fn reserve_decode_batch(&mut self, max_rows: usize) -> Result<usize, QueryError> {
        self.decoded = self.reader.memory_charge();
        let cell_bytes = self
            .ncols
            .saturating_mul(std::mem::size_of::<Value>())
            .saturating_mul(2);
        self.batch_row_cap = max_rows.min((self.reader.decode_limit() / cell_bytes.max(1)).max(1));
        self.batch_fixed_bytes = self.batch_row_cap.saturating_mul(cell_bytes);
        self.decoded.add_force(self.batch_fixed_bytes)?;
        Ok(self.batch_row_cap)
    }

    pub(crate) fn next_owned(
        &mut self,
    ) -> Result<
        Option<(
            Vec<Value>,
            crate::engine::executor::mem_budget::ScopedCharge,
        )>,
        QueryError,
    > {
        let Some(row) = self.next()? else {
            return Ok(None);
        };
        Ok(Some((row, self.take_decode_charge())))
    }

    pub(crate) fn is_exhausted(&self) -> bool {
        self.reader.remaining_bytes() == 0
    }

    pub(crate) fn decode_limit(&self) -> usize {
        self.reader.decode_limit()
    }

    pub(crate) fn take_decode_charge(
        &mut self,
    ) -> crate::engine::executor::mem_budget::ScopedCharge {
        let empty = self.decoded.sibling();
        std::mem::replace(&mut self.decoded, empty)
    }

    /// Decode up to `max_rows` straight into caller-owned column buffers.
    /// Grace replay is columnar above this boundary, so constructing and
    /// dropping one intermediate `Vec<Value>` for every spilled row only adds
    /// allocator traffic. Values remain decoded by the canonical spill oracle
    /// and `ColumnVector::from_values` still chooses the physical lane.
    pub(crate) fn read_columns(
        &mut self,
        columns: &mut [Vec<Value>],
        max_rows: usize,
    ) -> Result<usize, QueryError> {
        if columns.len() != self.ncols {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "Grace spill schema width does not match replay columns",
            ));
        }
        if self.decoded.charged() == 0 {
            self.reserve_decode_batch(max_rows)?;
        }
        let mut rows = 0usize;
        while rows < max_rows.min(self.batch_row_cap)
            && (rows == 0
                || self
                    .decoded
                    .charged()
                    .saturating_sub(self.batch_fixed_bytes)
                    < self.reader.decode_limit())
        {
            let mut first_tag = [0u8; 1];
            match self.reader.read(&mut first_tag) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) => return Err(io_err(error)),
            }
            if self.ncols == 0 {
                if first_tag[0] != VALUE_NULL {
                    return Err(
                        crate::error::catalog::ER_UNKNOWN_ERROR.error("invalid empty spill row")
                    );
                }
                rows += 1;
                continue;
            }
            columns[0].push(
                crate::engine::executor::spill_value::read_payload_accounted(
                    &mut self.reader,
                    first_tag[0],
                    false,
                    io_err,
                    Some(&mut self.decoded),
                )?,
            );
            for column in columns.iter_mut().skip(1) {
                column.push(read_value_accounted(&mut self.reader, &mut self.decoded)?);
            }
            rows += 1;
        }
        Ok(rows)
    }

    /// Decode directly into schema-selected typed accumulators. This removes
    /// the second full pass through `Vec<Value>` that `ColumnVector::from_values`
    /// otherwise performs for every Grace replay batch. The canonical value
    /// decoder remains the format/semantics oracle; only the transient layout
    /// changes.
    pub(crate) fn read_accumulators(
        &mut self,
        columns: &mut [ColumnAccumulator],
        max_rows: usize,
    ) -> Result<usize, QueryError> {
        if columns.len() != self.ncols {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "Grace spill schema width does not match replay accumulators",
            ));
        }
        if self.decoded.charged() == 0 {
            self.reserve_decode_batch(max_rows)?;
        }
        let mut rows = 0usize;
        while rows < max_rows.min(self.batch_row_cap)
            && (rows == 0
                || self
                    .decoded
                    .charged()
                    .saturating_sub(self.batch_fixed_bytes)
                    < self.reader.decode_limit())
        {
            let mut first_tag = [0u8; 1];
            match self.reader.read(&mut first_tag) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) => return Err(io_err(error)),
            }
            if self.ncols == 0 {
                if first_tag[0] != VALUE_NULL {
                    return Err(
                        crate::error::catalog::ER_UNKNOWN_ERROR.error("invalid empty spill row")
                    );
                }
                rows += 1;
                continue;
            }
            let first = crate::engine::executor::spill_value::read_payload_accounted(
                &mut self.reader,
                first_tag[0],
                false,
                io_err,
                Some(&mut self.decoded),
            )?;
            use crate::engine::executor::memory_footprint::AllocatedBytes;
            self.decoded.add_force(first.allocated_bytes())?;
            columns[0].push(&first);
            for column in columns.iter_mut().skip(1) {
                let value = read_value_accounted(&mut self.reader, &mut self.decoded)?;
                self.decoded.add_force(value.allocated_bytes())?;
                column.push(&value);
            }
            rows += 1;
        }
        Ok(rows)
    }
}

/// RAII guard for a temp directory: recursively deletes it on drop.
pub(crate) struct SpillTmpDir {
    path: PathBuf,
    /// P0-3 — temp-disk quota charger for every spill file written under
    /// this directory. Built at creation (which performs the up-front
    /// free-space check); its live bytes are released on drop, after the
    /// files are removed.
    charger: crate::engine::executor::spill_quota::SpillCharger,
}

const SPILL_OWNER_MARKER: &str = ".mydb-spill-owner";

impl SpillTmpDir {
    pub(super) fn create(parent_data_dir: &Path) -> Result<Self, QueryError> {
        Self::create_kind(parent_data_dir, "hashjoin")
    }

    /// Create a temp spill directory under `<data_dir>/tmp/` with the
    /// given kind prefix (e.g. `"hashjoin"`, `"sort"`). Used so that
    /// `cleanup_orphan_tmp_dirs` knows which directories are ours.
    pub(crate) fn create_kind(
        parent_data_dir: &Path,
        kind: &'static str,
    ) -> Result<Self, QueryError> {
        // P0-3 - refuse to start spilling if the volume is already below
        // the reserve threshold (1041 rather than a later ENOSPC).
        let parent = crate::storage::naming::system_tmp_dir(parent_data_dir);
        std::fs::create_dir_all(&parent).map_err(io_err)?;
        let charger = crate::engine::executor::spill_quota::charger_for_new_dir(&parent)?;
        let pid = std::process::id();
        let seq = NEXT_SPILL_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!("{}-{}-{}", kind, pid, seq));
        std::fs::create_dir(&path).map_err(io_err)?;
        let marker = format!(
            "pid={pid}\nquery_id={}\nkind={kind}\n",
            charger.owner_query_id().unwrap_or(0)
        );
        let marker_result = (|| -> Result<(), QueryError> {
            let mut writer = crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
                &path.join(SPILL_OWNER_MARKER),
                charger.clone(),
                32 * 1024,
            )?;
            writer.write_all(marker.as_bytes()).map_err(io_err)?;
            writer.finish()?;
            Ok(())
        })();
        if let Err(error) = marker_result {
            charger.retire_directory(path);
            return Err(error);
        }
        crate::engine::executor::spill_quota::record_operator_spill(kind);
        Ok(Self { path, charger })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// A cheap clonable handle that charges spill writes under this
    /// directory against the temp-disk quota.
    pub(crate) fn charger(&self) -> crate::engine::executor::spill_quota::SpillCharger {
        self.charger.clone()
    }
}

impl Drop for SpillTmpDir {
    fn drop(&mut self) {
        // File leases close their descriptors before the final directory
        // deletion. This also handles owners dropped before buffered writers.
        self.charger.retire_directory(self.path.clone());
    }
}

static NEXT_SPILL_SEQ: AtomicU64 = AtomicU64::new(0);

/// Clean leftover `<data_dir>/tmp/<kind>-*` directories at startup.
/// Temp partition files left by a previously crashed process get removed so they don't waste disk space.
/// Both hash-join and external-sort spill directories follow the same
/// `<kind>-<pid>-<seq>` naming so this catches any of them.
pub(crate) fn cleanup_orphan_tmp_dirs(data_dir: &Path) {
    let tmp = crate::storage::naming::system_tmp_dir(data_dir);
    let Ok(entries) = std::fs::read_dir(&tmp) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let s = name.to_string_lossy();
        let marked = entry.path().join(SPILL_OWNER_MARKER).is_file();
        // Keep legacy prefixes for directories created by an older binary;
        // every new spill kind is discovered through the marker.
        let legacy = s.starts_with("hashjoin-")
            || s.starts_with("sort-")
            || s.starts_with("indexsort-")
            || s.starts_with("hashagg-")
            || s.starts_with("cte-")
            || s == "temp_snapshots"
            || s == "protocol";
        if (marked || legacy) && entry.path().is_dir() {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_removes_crashed_protocol_spills() {
        let data = tempfile::tempdir().unwrap();
        let protocol = crate::storage::naming::system_tmp_dir(data.path()).join("protocol");
        std::fs::create_dir_all(&protocol).unwrap();
        std::fs::write(protocol.join("local-infile-1-1.tmp"), b"staged upload").unwrap();
        cleanup_orphan_tmp_dirs(data.path());
        assert!(!protocol.exists());
    }

    #[derive(Default)]
    struct CountingWriter {
        bytes: Vec<u8>,
        writes: usize,
    }

    impl Write for CountingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.writes += 1;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn staged_row_writer_coalesces_small_values_and_streams_large_values() {
        let mut actual = CountingWriter::default();
        {
            let mut staged = StagedRowWriter::<_, 32>::new(&mut actual);
            staged.write_all(&[1; 8]).unwrap();
            staged.write_all(&[2; 8]).unwrap();
            staged.write_all(&[3; 40]).unwrap();
            staged.write_all(&[4; 8]).unwrap();
            staged.finish_row().unwrap();
        }
        assert_eq!(
            actual.bytes,
            [[1; 8].as_slice(), &[2; 8], &[3; 40], &[4; 8]].concat()
        );
        assert_eq!(actual.writes, 3, "two small prefixes must share one write");
    }

    #[test]
    fn typed_batch_row_spill_matches_canonical_value_format() {
        use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
        use std::sync::Arc;

        let source = vec![
            Value::Int(-7),
            Value::UInt(u64::MAX),
            Value::Double(1.25),
            Value::DecimalCompact(12345, 2),
            Value::Decimal128(i128::from(i64::MAX) + 1, 3),
            Value::Date(20_000),
            Value::Time(123_456),
            Value::DateTime(987_654),
            Value::Timestamp(456_789),
            Value::Year(24),
            Value::String("spill text".into()),
            Value::Bytes(vec![0, 1, 255]),
            Value::Json(br#"{"k":1}"#.to_vec()),
            Value::Geometry(vec![9, 8, 7]),
            Value::Vector(vec![6, 5, 4]),
            Value::Null,
        ];
        let columns = source
            .into_iter()
            .map(|value| ColumnVector::from_values(vec![value]))
            .collect::<Vec<_>>();
        let expected = columns
            .iter()
            .map(|column| column.value_at(0))
            .collect::<Vec<_>>();
        let fields = columns
            .iter()
            .enumerate()
            .map(|(index, _)| BatchField {
                table: "t".into(),
                alias: None,
                name: format!("c{index}"),
                column_type: ColumnType::VarString,
                source: Default::default(),
            })
            .collect();
        let batch = Batch::new(Arc::new(BatchSchema::new(fields)), columns);

        let dir = tempfile::tempdir().unwrap();
        let tmp = SpillTmpDir::create(dir.path()).unwrap();
        let mut pool = LazyValueRowWriterPool::new(
            tmp.path(),
            "typed",
            1,
            batch.columns().len(),
            tmp.charger(),
            1,
        );
        pool.append_batch_row(0, &batch, 0).unwrap();
        pool.finish().unwrap();
        let mut reader = ValueRowReader::open(&tmp.path().join("typed-0"), tmp.charger())
            .unwrap()
            .unwrap();
        assert_eq!(reader.next().unwrap(), Some(expected.clone()));
        assert_eq!(reader.next().unwrap(), None);

        let mut reader = ValueRowReader::open(&tmp.path().join("typed-0"), tmp.charger())
            .unwrap()
            .unwrap();
        let mut replay_columns = (0..batch.columns().len())
            .map(|_| Vec::with_capacity(1))
            .collect::<Vec<_>>();
        assert_eq!(reader.read_columns(&mut replay_columns, 1024).unwrap(), 1);
        assert_eq!(
            replay_columns
                .into_iter()
                .map(|mut column| column.pop().unwrap())
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn value_row_reader_replays_directly_into_typed_accumulators() {
        use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
        use std::sync::Arc;

        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "k".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                source: Default::default(),
            },
        ]));
        let batch = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::from_values(vec![Value::Int(7), Value::Null]),
                ColumnVector::from_values(vec![
                    Value::DecimalCompact(1234, 2),
                    Value::DecimalCompact(-5, 2),
                ]),
            ],
        );
        let dir = tempfile::tempdir().unwrap();
        let tmp = SpillTmpDir::create(dir.path()).unwrap();
        let mut pool = LazyValueRowWriterPool::new(
            tmp.path(),
            "accum",
            1,
            batch.columns().len(),
            tmp.charger(),
            1,
        );
        pool.append_batch_row(0, &batch, 0).unwrap();
        pool.append_batch_row(0, &batch, 1).unwrap();
        pool.finish().unwrap();

        let mut reader = ValueRowReader::open(&tmp.path().join("accum-0"), tmp.charger())
            .unwrap()
            .unwrap();
        let mut accumulators = schema
            .fields()
            .iter()
            .map(|field| ColumnAccumulator::for_field(field, 4))
            .collect::<Vec<_>>();
        assert_eq!(reader.read_accumulators(&mut accumulators, 4).unwrap(), 2);
        let columns = accumulators
            .into_iter()
            .map(ColumnAccumulator::finish)
            .collect::<Vec<_>>();
        assert_eq!(columns[0].value_at(0), Value::Int(7));
        assert_eq!(columns[0].value_at(1), Value::Null);
        assert_eq!(columns[1].value_at(0), Value::DecimalCompact(1234, 2));
        assert_eq!(columns[1].value_at(1), Value::DecimalCompact(-5, 2));
    }

    #[test]
    fn recursive_partitioning_splits_a_parent_power_of_two_partition() {
        const PARTITIONS: u64 = 16;
        let parent_hashes = (0..100_000u64)
            .filter(|hash| partition_for_hash(*hash, 0, PARTITIONS) == 3)
            .take(512)
            .collect::<Vec<_>>();
        assert_eq!(parent_hashes.len(), 512);

        let mut children = [0usize; PARTITIONS as usize];
        for hash in parent_hashes {
            children[partition_for_hash(hash, 1, PARTITIONS) as usize] += 1;
        }
        let populated = children.into_iter().filter(|count| *count > 0).count();
        assert!(
            populated >= 12,
            "recursive rehash collapsed the parent into only {populated} children"
        );
    }

    #[test]
    fn partition_round_trip_preserves_full_column_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = SpillTmpDir::create(dir.path()).unwrap();
        let path = tmp.path().join("meta");
        let source = crate::types::ColumnSource {
            collation_coercibility: Some(0),
            dynamic_parameter: true,
            wire_type: Some(crate::mysql::protocol::MYSQL_TYPE_STRING),
            schema: "db".into(),
            table: "alias_t".into(),
            org_table: "t".into(),
            org_name: "secret".into(),
            flags: 0x23,
            length: Some(64),
            numeric_precision: Some(12),
            decimals: Some(3),
            collation_id: Some(crate::charset::collation_id("utf8mb4_bin")),
        };
        let meta = ColMeta {
            table: "t".into(),
            alias: Some("alias_t".into()),
            column: "secret".into(),
            column_type: ColumnType::VarString,
            source,
            invisible: true,
        };
        let row = PlanRow {
            fields: vec![PlanField {
                meta: super::super::plan::intern_col_meta(meta.clone()),
                value: Value::String("A".into()),
            }],
            ..Default::default()
        };
        let mut writer = PartitionWriter::create_charged(&path, tmp.charger()).unwrap();
        writer.append_row(&row).unwrap();
        writer.append_row(&row).unwrap();
        writer.finish().unwrap();
        let rows = read_partition(&path, tmp.charger()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(*rows[0].fields[0].meta, meta);
        assert_eq!(rows[0].fields[0].value, Value::String("A".into()));
        assert!(rows[0].fields[0].meta.ptr_eq(&rows[1].fields[0].meta));
    }

    #[test]
    fn lazy_value_partition_pool_caps_open_files_and_reopens_losslessly() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = SpillTmpDir::create(dir.path()).unwrap();
        let active_before = crate::engine::executor::spill_quota::active_spill_files();
        let mut pool = LazyValueRowWriterPool::new(tmp.path(), "value", 64, 1, tmp.charger(), 16);
        for pass in 0..2i64 {
            for partition in 0..64usize {
                pool.append(partition, &[Value::Int(pass * 100 + partition as i64)])
                    .unwrap();
                assert!(
                    crate::engine::executor::spill_quota::active_spill_files()
                        <= active_before + 16,
                    "lazy pool exceeded its descriptor bound"
                );
            }
        }
        assert!(pool.finish().unwrap() > 0);
        assert_eq!(
            crate::engine::executor::spill_quota::active_spill_files(),
            active_before
        );

        for partition in 0..64usize {
            let mut reader = ValueRowReader::open(
                &tmp.path().join(format!("value-{partition}")),
                tmp.charger(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(
                reader.next().unwrap(),
                Some(vec![Value::Int(partition as i64)])
            );
            assert_eq!(
                reader.next().unwrap(),
                Some(vec![Value::Int(100 + partition as i64)])
            );
            assert_eq!(reader.next().unwrap(), None);
        }
    }
}

#[cfg(test)]
mod spill_contract_regressions {
    use super::*;

    #[test]
    fn truncated_row_headers_are_errors() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let dir = SpillTmpDir::create_kind(root.path(), "audit").unwrap();
        for bytes in 1..=3 {
            let path = dir.path().join(format!("partial-{bytes}"));
            let mut writer = crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
                &path,
                dir.charger(),
                32 * 1024,
            )
            .unwrap();
            writer.write_all(&[1, 0, 0][..bytes]).unwrap();
            writer.finish().unwrap();
            assert!(ValueRowReader::open(&path, dir.charger()).is_err());
            assert!(PartitionReader::open(&path, dir.charger(), 1).is_err());
        }
    }
}
