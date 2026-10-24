//! Immutable query-owned row spool with a disk index and checked records.
//!
//! Offsets stay on disk. Readers admit only the row they return and keep the
//! creating query's charge, including when opened on another worker thread.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::types::{QueryError, Value};

use super::mem_budget::{self, ScopedCharge};
use super::select::grace::SpillTmpDir;
use super::spill_quota::{self, TrackedSpillReader, TrackedSpillWriter};
use super::spill_value;

const DATA_MAGIC: &[u8; 8] = b"MDBROW01";
const INDEX_MAGIC: &[u8; 8] = b"MDBIDX01";
const END_MAGIC: &[u8; 8] = b"MDBEND01";
const HEADER_BYTES: u64 = 32;
const RECORD_BYTES: u64 = 24;
const FOOTER_BYTES: u64 = 40;
static NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);

fn invalid(message: &str) -> QueryError {
    crate::error::catalog::ER_UNKNOWN_ERROR.error(format!("row spool: {message}"))
}

fn io_error(error: std::io::Error) -> QueryError {
    spill_quota::query_error_from_io(&error).unwrap_or_else(|| invalid(&format!("I/O: {error}")))
}

fn header(magic: &[u8; 8], columns: u32, token: u64) -> [u8; 32] {
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(magic);
    bytes[8..12].copy_from_slice(&1u32.to_le_bytes());
    bytes[12..16].copy_from_slice(&columns.to_le_bytes());
    bytes[16..24].copy_from_slice(&token.to_le_bytes());
    let crc = crc32fast::hash(&bytes[..28]);
    bytes[28..].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn footer(rows: u64, end: u64, payload: u64, mut hash: crc32fast::Hasher) -> [u8; 40] {
    let mut bytes = [0; 40];
    bytes[..8].copy_from_slice(END_MAGIC);
    bytes[8..16].copy_from_slice(&rows.to_le_bytes());
    bytes[16..24].copy_from_slice(&end.to_le_bytes());
    bytes[24..32].copy_from_slice(&payload.to_le_bytes());
    hash.update(&bytes[..32]);
    hash.update(&bytes[36..]);
    bytes[32..36].copy_from_slice(&hash.finalize().to_le_bytes());
    bytes
}

fn record_hash(token: u64, row: u64) -> crc32fast::Hasher {
    let mut hash = crc32fast::Hasher::new();
    hash.update(&token.to_le_bytes());
    hash.update(&row.to_le_bytes());
    hash
}

struct CountHash {
    len: u64,
    hash: crc32fast::Hasher,
}

impl Write for CountHash {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.len = self.len.checked_add(bytes.len() as u64).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "spill record length overflow",
            )
        })?;
        self.hash.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) struct RowSpoolBuilder {
    // Descriptors close before the directory can be retired.
    data: TrackedSpillWriter,
    index: TrackedSpillWriter,
    dir: Arc<SpillTmpDir>,
    columns: u32,
    token: u64,
    rows: u64,
    offset: u64,
    payload: u64,
    index_hash: crc32fast::Hasher,
    failed: bool,
}

impl RowSpoolBuilder {
    pub(crate) fn new(data_dir: &Path, columns: usize) -> Result<Self, QueryError> {
        let columns = u32::try_from(columns).map_err(|_| invalid("too many columns"))?;
        let dir = Arc::new(SpillTmpDir::create_kind(data_dir, "rowspool")?);
        let capacity = spill_quota::writer_buffer_capacity(2);
        let mut data =
            TrackedSpillWriter::create_new(&dir.path().join("rows"), dir.charger(), capacity)?;
        let mut index =
            TrackedSpillWriter::create_new(&dir.path().join("offsets"), dir.charger(), capacity)?;
        let token = NEXT_TOKEN.fetch_add(1, Ordering::Relaxed);
        data.write_all(&header(DATA_MAGIC, columns, token))
            .map_err(io_error)?;
        index
            .write_all(&header(INDEX_MAGIC, columns, token))
            .map_err(io_error)?;
        Ok(Self {
            data,
            index,
            dir,
            columns,
            token,
            rows: 0,
            offset: HEADER_BYTES,
            payload: 0,
            index_hash: crc32fast::Hasher::new(),
            failed: false,
        })
    }

    pub(crate) fn append(&mut self, values: &[Value]) -> Result<(), QueryError> {
        crate::engine::cancel::check_cancelled()?;
        if self.failed {
            return Err(invalid("writer has an incomplete record"));
        }
        if values.len() != self.columns as usize {
            return Err(invalid("row width mismatch"));
        }
        let mut count = CountHash {
            len: 0,
            hash: record_hash(self.token, self.rows),
        };
        for value in values {
            spill_value::write_value(&mut count, value).map_err(io_error)?;
        }
        let next = self
            .offset
            .checked_add(RECORD_BYTES)
            .and_then(|n| n.checked_add(count.len))
            .ok_or_else(|| invalid("file length overflow"))?;
        let payload = self
            .payload
            .checked_add(count.len)
            .ok_or_else(|| invalid("payload length overflow"))?;
        let rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| invalid("row count overflow"))?;
        let mut record = [0; 24];
        record[..8].copy_from_slice(&count.len.to_le_bytes());
        record[8..16].copy_from_slice(&self.rows.to_le_bytes());
        record[16..20].copy_from_slice(&count.hash.finalize().to_le_bytes());
        let crc = crc32fast::hash(&record[..20]);
        record[20..24].copy_from_slice(&crc.to_le_bytes());
        self.failed = true;
        self.data.write_all(&record).map_err(io_error)?;
        for value in values {
            spill_value::write_value(&mut self.data, value).map_err(io_error)?;
        }
        let offset = self.offset.to_le_bytes();
        self.index.write_all(&offset).map_err(io_error)?;
        self.index_hash.update(&offset);
        self.offset = next;
        self.payload = payload;
        self.rows = rows;
        self.failed = false;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<Arc<RowSpool>, QueryError> {
        if self.failed {
            return Err(invalid("cannot finish an incomplete record"));
        }
        self.data
            .write_all(&footer(
                self.rows,
                self.offset,
                self.payload,
                crc32fast::Hasher::new(),
            ))
            .map_err(io_error)?;
        let index_end = self
            .rows
            .checked_mul(8)
            .and_then(|n| n.checked_add(HEADER_BYTES))
            .ok_or_else(|| invalid("index length overflow"))?;
        self.index
            .write_all(&footer(self.rows, index_end, self.offset, self.index_hash))
            .map_err(io_error)?;
        let data_bytes = self.data.finish()?;
        let index_bytes = self.index.finish()?;
        Ok(Arc::new(RowSpool {
            dir: self.dir,
            columns: self.columns,
            token: self.token,
            rows: self.rows,
            data_end: self.offset,
            payload: self.payload,
            data_bytes,
            index_bytes,
        }))
    }
}

pub(crate) struct RowSpool {
    dir: Arc<SpillTmpDir>,
    columns: u32,
    token: u64,
    rows: u64,
    data_end: u64,
    payload: u64,
    data_bytes: u64,
    index_bytes: u64,
}

impl RowSpool {
    #[cfg(test)]
    pub(crate) fn len(&self) -> u64 {
        self.rows
    }
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> u64 {
        self.data_bytes.saturating_add(self.index_bytes)
    }

    pub(crate) fn reader(self: &Arc<Self>) -> Result<RowSpoolReader, QueryError> {
        let capacity = spill_quota::writer_buffer_capacity(2);
        let mut data =
            TrackedSpillReader::open(&self.dir.path().join("rows"), self.dir.charger(), capacity)?;
        let mut index = TrackedSpillReader::open(
            &self.dir.path().join("offsets"),
            self.dir.charger(),
            capacity,
        )?;
        if data.remaining_bytes() != self.data_bytes || index.remaining_bytes() != self.index_bytes
        {
            return Err(invalid("file length mismatch"));
        }
        let mut bytes = [0; 32];
        data.read_exact(&mut bytes).map_err(io_error)?;
        if bytes != header(DATA_MAGIC, self.columns, self.token) {
            return Err(invalid("invalid data header"));
        }
        index.read_exact(&mut bytes).map_err(io_error)?;
        if bytes != header(INDEX_MAGIC, self.columns, self.token) {
            return Err(invalid("invalid index header"));
        }
        data.seek(SeekFrom::Start(self.data_end))
            .map_err(io_error)?;
        let mut end = [0; 40];
        data.read_exact(&mut end).map_err(io_error)?;
        if end
            != footer(
                self.rows,
                self.data_end,
                self.payload,
                crc32fast::Hasher::new(),
            )
        {
            return Err(invalid("invalid data footer"));
        }
        // One bounded scan validates the tiny disk index; no O(N) offset Vec
        // is retained and random reads validate the requested row ordinal.
        let mut hash = crc32fast::Hasher::new();
        let mut remaining = self
            .rows
            .checked_mul(8)
            .ok_or_else(|| invalid("index length overflow"))?;
        let mut buffer = [0; 8192];
        while remaining > 0 {
            crate::engine::cancel::check_cancelled()?;
            let n = remaining.min(buffer.len() as u64) as usize;
            index.read_exact(&mut buffer[..n]).map_err(io_error)?;
            hash.update(&buffer[..n]);
            remaining -= n as u64;
        }
        index.read_exact(&mut end).map_err(io_error)?;
        let index_end = self.index_bytes - FOOTER_BYTES;
        if end != footer(self.rows, index_end, self.data_end, hash) {
            return Err(invalid("invalid index footer"));
        }
        data.seek(SeekFrom::Start(HEADER_BYTES)).map_err(io_error)?;
        Ok(RowSpoolReader {
            data,
            #[cfg(test)]
            index,
            spool: self.clone(),
            next: 0,
            data_position: HEADER_BYTES,
        })
    }
}

pub(crate) struct OwnedRow {
    pub(crate) values: Vec<Value>,
    pub(crate) charge: ScopedCharge,
}

impl OwnedRow {
    /// Move one indivisible row into an owning batch, including zero-column
    /// rows. Generic lanes retain the original value tags and allocate only
    /// one cell per column instead of an entire default batch of slots.
    pub(crate) fn into_batch(
        mut self,
        schema: Arc<super::vec::schema::BatchSchema>,
    ) -> Result<super::vec::batch::Batch, QueryError> {
        use super::vec::batch::{Batch, ColumnVector};
        if self.values.len() != schema.len() {
            return Err(invalid("owned row batch schema mismatch"));
        }
        self.charge.add_force(
            schema
                .len()
                .saturating_mul(std::mem::size_of::<ColumnVector>() + std::mem::size_of::<Value>())
                .saturating_add(128),
        )?;
        let mut columns = mem_budget::try_vec_with_capacity(schema.len(), "owned row columns")?;
        for value in self.values {
            let mut values = mem_budget::try_vec_with_capacity(1, "owned row lane")?;
            values.push(value);
            columns.push(ColumnVector::Generic { values });
        }
        Batch::with_row_count(schema, columns, 1).with_memory_charge(self.charge)
    }
}

pub(crate) struct RowSpoolReader {
    data: TrackedSpillReader,
    #[cfg(test)]
    index: TrackedSpillReader,
    spool: Arc<RowSpool>,
    next: u64,
    data_position: u64,
}

struct CheckedRecord<'a> {
    reader: &'a mut TrackedSpillReader,
    remaining: u64,
    hash: crc32fast::Hasher,
}

impl Read for CheckedRecord<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let count = bytes
            .len()
            .min(self.remaining.min(usize::MAX as u64) as usize);
        if count == 0 {
            return Ok(0);
        }
        let read = self.reader.read(&mut bytes[..count])?;
        self.remaining -= read as u64;
        self.hash.update(&bytes[..read]);
        Ok(read)
    }
}

impl RowSpoolReader {
    pub(crate) fn next(&mut self) -> Result<Option<OwnedRow>, QueryError> {
        if self.next == self.spool.rows {
            if self.data_position != self.spool.data_end {
                return Err(invalid("unexpected bytes after last row"));
            }
            return Ok(None);
        }
        let row = self.read_record(self.next)?;
        self.next += 1;
        Ok(Some(row))
    }

    #[cfg(test)]
    pub(crate) fn row(&mut self, ordinal: u64) -> Result<OwnedRow, QueryError> {
        if ordinal >= self.spool.rows {
            return Err(invalid("row index out of bounds"));
        }
        self.index
            .seek(SeekFrom::Start(HEADER_BYTES + ordinal * 8))
            .map_err(io_error)?;
        let mut offset = [0; 8];
        self.index.read_exact(&mut offset).map_err(io_error)?;
        let offset = u64::from_le_bytes(offset);
        if offset < HEADER_BYTES || offset > self.spool.data_end.saturating_sub(RECORD_BYTES) {
            return Err(invalid("record offset out of bounds"));
        }
        self.data.seek(SeekFrom::Start(offset)).map_err(io_error)?;
        self.data_position = offset;
        let row = self.read_record(ordinal)?;
        self.next = ordinal + 1;
        Ok(row)
    }

    fn read_record(&mut self, ordinal: u64) -> Result<OwnedRow, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        let mut header = [0; 24];
        self.data.read_exact(&mut header).map_err(io_error)?;
        let len = u64::from_le_bytes(header[..8].try_into().unwrap());
        let saved_ordinal = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let checksum = u32::from_le_bytes(header[16..20].try_into().unwrap());
        if saved_ordinal != ordinal
            || crc32fast::hash(&header[..20])
                != u32::from_le_bytes(header[20..].try_into().unwrap())
        {
            return Err(invalid("record header checksum or ordinal mismatch"));
        }
        let end = self
            .data_position
            .checked_add(RECORD_BYTES)
            .and_then(|n| n.checked_add(len))
            .filter(|end| *end <= self.spool.data_end)
            .ok_or_else(|| invalid("record length exceeds file"))?;
        if len < self.spool.columns as u64 {
            return Err(invalid("record too short for its schema"));
        }
        let mut charge = self.data.memory_charge();
        charge.add_force(
            (self.spool.columns as usize)
                .checked_mul(std::mem::size_of::<Value>())
                .and_then(|n| n.checked_add(std::mem::size_of::<Vec<Value>>()))
                .ok_or_else(|| invalid("decoded row size overflow"))?,
        )?;
        let mut values =
            mem_budget::try_vec_with_capacity(self.spool.columns as usize, "spool row")?;
        let mut record = CheckedRecord {
            reader: &mut self.data,
            remaining: len,
            hash: record_hash(self.spool.token, ordinal),
        };
        for _ in 0..self.spool.columns {
            let mut tag = [0];
            record.read_exact(&mut tag).map_err(io_error)?;
            let remaining = record.remaining;
            values.push(spill_value::read_payload_limited(
                &mut record,
                tag[0],
                true,
                io_error,
                Some(&mut charge),
                Some(remaining),
            )?);
        }
        if record.remaining != 0 || record.hash.finalize() != checksum {
            return Err(invalid("record checksum or payload length mismatch"));
        }
        self.data_position = end;
        Ok(OwnedRow { values, charge })
    }
}

#[cfg(test)]
mod tests {
    use super::super::mem_budget::{with_query_budget, MemBudget};
    use super::*;

    fn values() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Int(i64::MIN),
            Value::UInt(u64::MAX),
            Value::Float(f32::from_bits(0x7fc00123)),
            Value::Double(-0.0),
            Value::Decimal("123.450".into()),
            Value::DecimalCompact(-123, 2),
            Value::Decimal128(i128::MIN, 10),
            Value::DecimalWide(Box::new(crate::types::DecimalWide {
                magnitude: num_bigint::BigInt::from(i128::MAX) * 1000,
                scale: 4,
            })),
            Value::String("短文本".into()),
            Value::String("wide".repeat(256)),
            Value::SmallString(crate::types::SmallStringBuf::from_slice(b"inline")),
            Value::Bytes(vec![0, 255]),
            Value::Date(crate::types::ZERO_DATE),
            Value::Time(-123),
            Value::DateTime(crate::types::ZERO_DATETIME),
            Value::Timestamp(12345678),
            Value::Year(99),
            Value::Json(vec![1, 2]),
            Value::Geometry(vec![3, 4]),
            Value::Vector(vec![5, 6]),
        ]
    }

    fn encode(values: &[Value]) -> Vec<u8> {
        let mut encoded = Vec::new();
        for value in values {
            spill_value::write_value(&mut encoded, value).unwrap();
        }
        encoded
    }

    #[test]
    fn indexed_rows_round_trip_every_type_and_keep_original_owner() {
        let dir = tempfile::tempdir().unwrap();
        let budget = Arc::new(MemBudget::new(4 * 1024 * 1024));
        let expected = encode(&values());
        let spool = with_query_budget(budget.clone(), || {
            let mut builder = RowSpoolBuilder::new(dir.path(), values().len()).unwrap();
            for _ in 0..50 {
                builder.append(&values()).unwrap();
            }
            builder.finish().unwrap()
        });
        assert_eq!(spool.len(), 50);
        assert!(spool.bytes() > 0);
        let retained = std::thread::spawn(move || {
            let mut reader = spool.reader().unwrap();
            for ordinal in [49, 0, 23] {
                assert_eq!(encode(&reader.row(ordinal).unwrap().values), expected);
            }
            for _ in 24..50 {
                assert!(reader.next().unwrap().is_some());
            }
            assert!(reader.next().unwrap().is_none());
            assert!(reader.row(50).is_err());
            reader.row(1).unwrap()
        })
        .join()
        .unwrap();
        assert!(retained.charge.charged() >= 1024);
        assert_eq!(budget.used(), retained.charge.charged());
        drop(retained);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
    }

    #[test]
    fn empty_and_zero_column_spools_preserve_cardinality() {
        for count in [0, 1, 50] {
            let dir = tempfile::tempdir().unwrap();
            let mut builder = RowSpoolBuilder::new(dir.path(), 0).unwrap();
            for _ in 0..count {
                builder.append(&[]).unwrap();
            }
            let spool = builder.finish().unwrap();
            let mut reader = spool.reader().unwrap();
            for _ in 0..count {
                assert!(reader.next().unwrap().unwrap().values.is_empty());
            }
            assert!(reader.next().unwrap().is_none());
        }
    }

    #[test]
    fn every_byte_truncation_and_index_corruption_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = RowSpoolBuilder::new(dir.path(), 1).unwrap();
        for i in 0..3 {
            builder.append(&[Value::Int(i)]).unwrap();
        }
        let spool = builder.finish().unwrap();
        let data_path = spool.dir.path().join("rows");
        let index_path = spool.dir.path().join("offsets");
        for path in [&data_path, &index_path] {
            let bytes = std::fs::read(path).unwrap();
            for end in 0..bytes.len() {
                std::fs::write(path, &bytes[..end]).unwrap();
                assert!(
                    spool.reader().is_err(),
                    "accepted {} at {end}",
                    path.display()
                );
            }
            std::fs::write(path, &bytes).unwrap();
        }
        let bytes = std::fs::read(&index_path).unwrap();
        for byte in 0..bytes.len() {
            let mut corrupt = bytes.clone();
            corrupt[byte] ^= 1;
            std::fs::write(&index_path, corrupt).unwrap();
            assert!(
                spool.reader().is_err(),
                "accepted corrupt index byte {byte}"
            );
        }
        std::fs::write(&index_path, bytes).unwrap();
        let bytes = std::fs::read(&data_path).unwrap();
        for byte in 0..bytes.len() {
            let mut corrupt = bytes.clone();
            corrupt[byte] ^= 1;
            std::fs::write(&data_path, corrupt).unwrap();
            let result = (|| -> Result<(), QueryError> {
                let mut reader = spool.reader()?;
                while reader.next()?.is_some() {}
                Ok(())
            })();
            assert!(result.is_err(), "accepted corrupt data byte {byte}");
        }
        std::fs::write(&data_path, bytes).unwrap();
    }

    #[test]
    fn record_bounds_are_checked_before_payload_reservation() {
        let dir = tempfile::tempdir().unwrap();
        let budget = Arc::new(MemBudget::new(512 * 1024));
        with_query_budget(budget.clone(), || {
            let mut builder = RowSpoolBuilder::new(dir.path(), 1).unwrap();
            builder.append(&[Value::Bytes(vec![1, 2, 3])]).unwrap();
            let spool = builder.finish().unwrap();
            let path = spool.dir.path().join("rows");
            let mut bytes = std::fs::read(&path).unwrap();
            let value_length = (HEADER_BYTES + RECORD_BYTES + 1) as usize;
            bytes[value_length..value_length + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            // Keep the outer file checksum valid so this exercises record
            // decoding bounds independently of the earlier integrity check.
            let mut rewrite =
                TrackedSpillWriter::create_truncate(&path, spool.dir.charger(), 32 * 1024).unwrap();
            rewrite.write_all(&bytes).unwrap();
            rewrite.finish().unwrap();
            let mut reader = spool.reader().unwrap();
            let before = budget.used();
            let error = reader.next().err().expect("corrupt length must fail");
            assert!(error.message().contains("length exceeds its record"));
            assert_eq!(budget.used(), before);
            drop(reader);
            drop(spool);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.spill_bytes(), 0);
        });
    }

    #[test]
    fn unfinished_builder_discards_buffers_and_releases_its_directory() {
        let dir = tempfile::tempdir().unwrap();
        let budget = Arc::new(MemBudget::new(512 * 1024));
        with_query_budget(budget.clone(), || {
            let mut builder = RowSpoolBuilder::new(dir.path(), 1).unwrap();
            builder
                .append(&[Value::String("abort".repeat(100))])
                .unwrap();
            let path = builder.dir.path().to_path_buf();
            drop(builder);
            assert!(!path.exists());
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.spill_bytes(), 0);
        });
    }
}

pub(crate) struct RowSpoolBatches {
    reader: RowSpoolReader,
    schema: Arc<super::vec::schema::BatchSchema>,
}

impl RowSpoolBatches {
    pub(crate) fn new(
        spool: &Arc<RowSpool>,
        schema: Arc<super::vec::schema::BatchSchema>,
    ) -> Result<Self, QueryError> {
        if schema.len() != spool.columns as usize {
            return Err(invalid("batch schema width mismatch"));
        }
        Ok(Self {
            reader: spool.reader()?,
            schema,
        })
    }
}

impl super::vec::stream::BatchOperator for RowSpoolBatches {
    fn schema(&self) -> &Arc<super::vec::schema::BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<super::vec::batch::Batch>, QueryError> {
        use super::vec::batch::{Batch, ColumnVector, DEFAULT_BATCH_ROWS};
        let mut charge = self.reader.data.memory_charge();
        let mut rows = Vec::new();
        let limit = self.reader.data.decode_limit();
        while rows.len() < DEFAULT_BATCH_ROWS && (rows.is_empty() || charge.charged() < limit) {
            let Some(mut row) = self.reader.next()? else {
                break;
            };
            mem_budget::try_reserve_vec_hard(&mut rows, 1, &mut charge, "spool output rows")?;
            charge.absorb(&mut row.charge);
            rows.push(row.values);
        }
        if rows.is_empty() {
            return Ok(None);
        }
        let count = rows.len();
        let slots = count
            .checked_mul(self.schema.len())
            .and_then(|n| n.checked_mul(std::mem::size_of::<Value>()))
            .and_then(|n| {
                n.checked_add(
                    self.schema
                        .len()
                        .saturating_mul(std::mem::size_of::<ColumnVector>() + 24),
                )
            })
            .ok_or_else(|| invalid("batch size overflow"))?;
        charge.add_force(slots)?;
        let mut columns =
            mem_budget::try_vec_with_capacity(self.schema.len(), "spool output columns")?;
        for _ in 0..self.schema.len() {
            columns.push(mem_budget::try_vec_with_capacity(
                count,
                "spool output lane",
            )?);
        }
        for row in rows {
            for (column, value) in columns.iter_mut().zip(row) {
                column.push(value);
            }
        }
        let columns = columns
            .into_iter()
            .map(|values| ColumnVector::Generic { values })
            .collect();
        Batch::with_row_count(self.schema.clone(), columns, count)
            .with_memory_charge(charge)
            .map(Some)
    }
}
