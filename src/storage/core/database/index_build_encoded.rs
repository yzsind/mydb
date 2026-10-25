/// Schema-compiled, type-complete index-key encoding shared by every B-tree
/// build path. Sort order bytes are deliberately separate from the canonical
/// materialization payload: collation weights are order-preserving but cannot
/// reconstruct the original string stored in an index-only leaf.
#[derive(Clone)]
pub(crate) struct CompiledIndexKeyProgram {
    index: IndexSchema,
    direct_parts: Option<Vec<CompiledIndexKeyPart>>,
    decode_schema: Vec<ParamTarget>,
    fixed_ops: Option<Vec<FixedKeyOp>>,
    fixed_signed_widths: Option<smallvec::SmallVec<[u8; 4]>>,
    physical_lane: EncodedPhysicalLane,
}

#[derive(Clone)]
struct CompiledIndexKeyPart {
    source_column_idx: usize,
    value_offset: usize,
    key_pos: usize,
}

#[derive(Debug, Clone, Copy)]
enum FixedKeyOp {
    Signed(u8),
    Unsigned(u8),
    Float32,
    Float64,
    DecimalI128 { scale: u8 },
    DecimalWide { scale: u8 },
    Date,
    Time,
    DateTime,
    Year,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EncodedPhysicalLane {
    Generic,
    SingleInt,
    PairInt,
    Date,
}

#[derive(Debug, Clone, Copy)]
enum OfflineProjectedScalar {
    Missing,
    Null,
    Signed(i64),
    Unsigned(u64),
    Float32(f32),
    Float64(f64),
    Decimal(i128, u8),
    Date(i32),
    Time(i64),
    DateTime(i64),
    Year(u8),
    Unsupported,
}

/// Typed projection target used by the encoded offline scan. Keeping it next
/// to the compiled key program gives every supported scalar type one common
/// decode-to-key route.
struct OfflineFixedDecodeSink<'a> {
    values: &'a mut [OfflineProjectedScalar],
}

impl OfflineFixedDecodeSink<'_> {
    #[inline]
    fn unsupported(&mut self, col: usize) {
        self.values[col] = OfflineProjectedScalar::Unsupported;
    }
}

impl crate::storage::RowDecodeSink for OfflineFixedDecodeSink<'_> {
    #[inline]
    fn put_null(&mut self, col: usize) {
        self.values[col] = OfflineProjectedScalar::Null;
    }
    #[inline]
    fn put_i64(&mut self, col: usize, value: i64) {
        self.values[col] = OfflineProjectedScalar::Signed(value);
    }
    #[inline]
    fn put_u64(&mut self, col: usize, value: u64) {
        self.values[col] = OfflineProjectedScalar::Unsigned(value);
    }
    #[inline]
    fn put_f32(&mut self, col: usize, value: f32) {
        self.values[col] = OfflineProjectedScalar::Float32(value);
    }
    #[inline]
    fn put_f64(&mut self, col: usize, value: f64) {
        self.values[col] = OfflineProjectedScalar::Float64(value);
    }
    #[inline]
    fn put_decimal_compact(&mut self, col: usize, magnitude: i64, scale: u8) {
        self.values[col] = OfflineProjectedScalar::Decimal(i128::from(magnitude), scale);
    }
    #[inline]
    fn put_decimal_wide(&mut self, col: usize, magnitude: i128, scale: u8) {
        self.values[col] = OfflineProjectedScalar::Decimal(magnitude, scale);
    }
    #[inline]
    fn put_date(&mut self, col: usize, days: i32) {
        self.values[col] = OfflineProjectedScalar::Date(days);
    }
    #[inline]
    fn put_year(&mut self, col: usize, year: u8) {
        self.values[col] = OfflineProjectedScalar::Year(year);
    }
    #[inline]
    fn put_time(&mut self, col: usize, micros: i64) {
        self.values[col] = OfflineProjectedScalar::Time(micros);
    }
    #[inline]
    fn put_datetime(&mut self, col: usize, micros: i64) {
        self.values[col] = OfflineProjectedScalar::DateTime(micros);
    }
    #[inline]
    fn put_bytes(&mut self, col: usize, _bytes: &[u8]) {
        self.unsupported(col);
    }
    #[inline]
    fn put_string(&mut self, col: usize, _bytes: &[u8]) {
        self.unsupported(col);
    }
    #[inline]
    fn put_json(&mut self, col: usize, _bytes: &[u8]) {
        self.unsupported(col);
    }
    #[inline]
    fn put_geometry(&mut self, col: usize, _bytes: &[u8]) {
        self.unsupported(col);
    }
    #[inline]
    fn put_vector(&mut self, col: usize, _bytes: &[u8]) {
        self.unsupported(col);
    }
    #[inline]
    fn put_value(&mut self, col: usize, value: Value) {
        self.values[col] = match value {
            Value::Int(value) => OfflineProjectedScalar::Signed(value),
            Value::Date(days) => OfflineProjectedScalar::Date(days),
            Value::Null => OfflineProjectedScalar::Null,
            _ => OfflineProjectedScalar::Unsupported,
        };
    }
}

impl FixedKeyOp {
    fn width(self) -> usize {
        1 + match self {
            Self::Signed(bytes) | Self::Unsigned(bytes) => usize::from(bytes),
            Self::Float32 | Self::Date => 4,
            Self::Float64 | Self::Time | Self::DateTime => 8,
            Self::DecimalI128 { .. } => 17,
            Self::DecimalWide { .. } => 29,
            Self::Year => 1,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EncodedPendingIndexRecord {
    order_key: smallvec::SmallVec<[u8; 32]>,
    materialized_key: Option<Vec<u8>>,
    has_null: bool,
}

impl EncodedPendingIndexRecord {
    pub(crate) fn capacity_bytes(&self) -> usize {
        self.order_key
            .spilled()
            .then_some(self.order_key.capacity())
            .unwrap_or(0)
            + self.materialized_key.as_ref().map_or(0, Vec::capacity)
    }
}

#[derive(Debug, Clone)]
struct EncodedIndexRecord {
    order_key: Vec<u8>,
    materialized_key: Option<Vec<u8>>,
    row_id: RowId,
    has_null: bool,
}

impl PartialEq for EncodedIndexRecord {
    fn eq(&self, other: &Self) -> bool {
        self.order_key == other.order_key && self.row_id == other.row_id
    }
}

impl Eq for EncodedIndexRecord {}

impl PartialOrd for EncodedIndexRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EncodedIndexRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order_key
            .cmp(&other.order_key)
            .then_with(|| self.row_id.cmp(&other.row_id))
    }
}

#[derive(Clone, Copy)]
struct EncodedVarDescriptor {
    order_start: u32,
    order_len: u32,
    payload_start: u32,
    payload_len: u32,
    row_id: RowId,
    has_null: bool,
}

const VARIABLE_DICTIONARY_SAMPLE_ROWS: usize = 65_536;
const VARIABLE_DICTIONARY_SAMPLE_DISTINCT_MAX: usize = VARIABLE_DICTIONARY_SAMPLE_ROWS / 8;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EncodedInternedKey {
    order_key: Vec<u8>,
    materialized_key: Option<Vec<u8>>,
    has_null: bool,
}

type EncodedInternedLookup = hashbrown::HashMap<EncodedInternedKey, u32, ahash::RandomState>;

fn interned_key_capacity_bytes(key: &EncodedInternedKey) -> usize {
    // Both the dictionary and hash lookup own a copy. Include descriptor and
    // hash-table slack, rather than counting only initialized string bytes.
    512 + 2 * (key.order_key.capacity() + key.materialized_key.as_ref().map_or(0, Vec::capacity))
}

/// Packed low-cardinality variable record. The high 32 bits are the
/// collation-order rank, followed by the six-byte RowId and the original
/// dictionary id. Keeping the complete sort identity in one `u128` lets the
/// existing parallel comparison sorter operate without per-row key objects.
#[inline]
fn pack_interned_record(rank: u32, key_id: u32, row_id: RowId) -> u128 {
    (u128::from(rank) << 96)
        | (u128::from(row_id.page_id) << 64)
        | (u128::from(row_id.slot_id) << 48)
        | (u128::from(key_id) << 16)
}

#[inline]
fn interned_record_rank(record: u128) -> u32 {
    (record >> 96) as u32
}

#[inline]
fn interned_record_key_id(record: u128) -> u32 {
    ((record >> 16) & u128::from(u32::MAX)) as u32
}

#[inline]
fn interned_record_row_id(record: u128) -> RowId {
    RowId {
        page_id: ((record >> 64) & u128::from(u32::MAX)) as u32,
        slot_id: ((record >> 48) & u128::from(u16::MAX)) as u16,
    }
}

enum EncodedSortStorage {
    Fixed128 {
        key_width: usize,
        stride: usize,
        records: Vec<u128>,
    },
    Fixed {
        key_width: usize,
        stride: usize,
        bytes: Vec<u8>,
        order: Vec<u32>,
    },
    Variable {
        arena: Vec<u8>,
        records: Vec<EncodedVarDescriptor>,
        dictionary_sampled: bool,
    },
    Interned {
        keys: Vec<EncodedInternedKey>,
        lookup: EncodedInternedLookup,
        records: Vec<u128>,
        dictionary_budget: usize,
        dictionary_bytes: usize,
    },
}

struct EncodedIndexSorter {
    program: Arc<CompiledIndexKeyProgram>,
    storage: EncodedSortStorage,
    run_bytes: usize,
    tmp: Option<IndexSortTmpDir>,
    runs: Vec<PathBuf>,
    sort_degree: usize,
}

enum EncodedSortedRecords {
    Fixed128 {
        key_width: usize,
        stride: usize,
        records: Vec<u128>,
    },
    Fixed {
        key_width: usize,
        stride: usize,
        bytes: Vec<u8>,
        order: Vec<u32>,
    },
    Variable {
        arena: Vec<u8>,
        records: Vec<EncodedVarDescriptor>,
    },
    Interned {
        keys: Vec<EncodedInternedKey>,
        records: Vec<u128>,
    },
    Spilled {
        tmp: IndexSortTmpDir,
        runs: Vec<PathBuf>,
        fixed128: Option<(usize, usize)>,
    },
}

enum EncodedSortedRecordIter {
    Fixed128(EncodedFixed128MemoryIter),
    Fixed(EncodedFixedMemoryIter),
    Variable(EncodedVariableMemoryIter),
    Interned(EncodedInternedMemoryIter),
    Spilled(EncodedRunMerger),
    Fixed128Spilled(EncodedFixed128RunIter),
}

struct EncodedFixed128MemoryIter {
    program: Arc<CompiledIndexKeyProgram>,
    key_width: usize,
    stride: usize,
    records: std::vec::IntoIter<u128>,
}

struct EncodedInternedMemoryIter {
    keys: Arc<[EncodedInternedKey]>,
    records: std::vec::IntoIter<u128>,
}

struct EncodedFixed128RunIter {
    merger: EncodedFixed128RunMerger,
    program: Arc<CompiledIndexKeyProgram>,
    key_width: usize,
}

struct ValidatedEncodedIndexIter {
    inner: ValidatedEncodedRecordIter,
    program: Arc<CompiledIndexKeyProgram>,
}

enum ValidatedEncodedIndexStream {
    Fixed128(ValidatedFixed128IndexIter),
    Fixed(ValidatedFixedIndexIter),
    Other(ValidatedEncodedIndexIter),
}

struct ValidatedFixed128IndexIter {
    records: std::vec::IntoIter<u128>,
    key_width: usize,
    stride: usize,
    program: Arc<CompiledIndexKeyProgram>,
    unique: bool,
    primary: bool,
    index_name: String,
    previous_key: Option<u128>,
}

struct ValidatedFixedIndexIter {
    bytes: Vec<u8>,
    order: std::vec::IntoIter<u32>,
    key_width: usize,
    stride: usize,
    program: Arc<CompiledIndexKeyProgram>,
    unique: bool,
    primary: bool,
    index_name: String,
    previous_ordinal: Option<u32>,
}

impl Iterator for ValidatedEncodedIndexStream {
    type Item = Result<IndexEntry, QueryError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Fixed128(iter) => iter.next(),
            Self::Fixed(iter) => iter.next(),
            Self::Other(iter) => iter.next(),
        }
    }
}

impl Iterator for ValidatedFixed128IndexIter {
    type Item = Result<IndexEntry, QueryError>;

    fn next(&mut self) -> Option<Self::Item> {
        let encoded = self.records.next()?;
        let bytes = encoded.to_be_bytes();
        let key_bytes = &bytes[..self.key_width];
        let has_null = (self.primary || self.unique)
            && self
                .program
                .fixed_ops
                .as_ref()
                .is_some_and(|ops| fixed_order_key_has_null(key_bytes, ops));
        if self.primary && has_null {
            return Some(Err(error(
                crate::error::catalog::ER_INVALID_USE_OF_NULL,
                "invalid use of NULL value for primary key",
            )));
        }
        let identity = encoded >> ((16 - self.key_width) * 8);
        if self.unique && !has_null && self.previous_key == Some(identity) {
            return Some(Err(error(
                crate::error::catalog::ER_DUP_ENTRY,
                format!("duplicate entry for key '{}'", self.index_name),
            )));
        }
        if self.unique && !has_null {
            self.previous_key = Some(identity);
        }
        let key = match decode_compiled_fixed_order_key(key_bytes, &self.program) {
            Ok(key) => key,
            Err(err) => return Some(Err(err)),
        };
        debug_assert!(self.stride <= 16);
        Some(Ok(IndexEntry {
            key,
            row_id: decode_fixed128_row_id(&bytes, self.key_width),
        }))
    }
}

impl Iterator for ValidatedFixedIndexIter {
    type Item = Result<IndexEntry, QueryError>;

    fn next(&mut self) -> Option<Self::Item> {
        let ordinal = self.order.next()?;
        let start = ordinal as usize * self.stride;
        let record = &self.bytes[start..start + self.stride];
        let key_bytes = &record[..self.key_width];
        let has_null = (self.primary || self.unique)
            && self
                .program
                .fixed_ops
                .as_ref()
                .is_some_and(|ops| fixed_order_key_has_null(key_bytes, ops));
        if self.primary && has_null {
            return Some(Err(error(
                crate::error::catalog::ER_INVALID_USE_OF_NULL,
                "invalid use of NULL value for primary key",
            )));
        }
        let duplicate = self.unique
            && !has_null
            && self.previous_ordinal.is_some_and(|previous| {
                let previous = previous as usize * self.stride;
                self.bytes[previous..previous + self.key_width] == *key_bytes
            });
        if duplicate {
            return Some(Err(error(
                crate::error::catalog::ER_DUP_ENTRY,
                format!("duplicate entry for key '{}'", self.index_name),
            )));
        }
        if self.unique && !has_null {
            self.previous_ordinal = Some(ordinal);
        }
        let key = match decode_compiled_fixed_order_key(key_bytes, &self.program) {
            Ok(key) => key,
            Err(err) => return Some(Err(err)),
        };
        Some(Ok(IndexEntry {
            key,
            row_id: RowId {
                page_id: u32::from_be_bytes(
                    record[self.key_width..self.key_width + 4]
                        .try_into()
                        .unwrap(),
                ),
                slot_id: u16::from_be_bytes(
                    record[self.key_width + 4..self.stride].try_into().unwrap(),
                ),
            },
        }))
    }
}

struct ValidatedEncodedRecordIter {
    inner: EncodedSortedRecordIter,
    unique: bool,
    primary: bool,
    index_name: String,
    previous: Option<Vec<u8>>,
}

struct ValidatedFixed128Iter {
    records: Fixed128Source,
    key_width: usize,
    stride: usize,
    program: Arc<CompiledIndexKeyProgram>,
    unique: bool,
    primary: bool,
    index_name: String,
    previous_key: Option<u128>,
}

enum Fixed128Source {
    Resident(std::vec::IntoIter<u128>),
    Runs(EncodedFixed128RunMerger),
}

impl Iterator for Fixed128Source {
    type Item = Result<u128, QueryError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Resident(records) => records.next().map(Ok),
            Self::Runs(merger) => merger.next(),
        }
    }
}

enum PrefetchedFixedEntries<I, T> {
    Direct(I),
    Buffered {
        receiver: std::sync::mpsc::Receiver<Result<Vec<T>, QueryError>>,
        current: std::vec::IntoIter<T>,
    },
}

impl<I, T> Iterator for PrefetchedFixedEntries<I, T>
where
    I: Iterator<Item = Result<T, QueryError>>,
{
    type Item = Result<T, QueryError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Direct(entries) => entries.next(),
            Self::Buffered { receiver, current } => loop {
                if let Some(entry) = current.next() {
                    return Some(Ok(entry));
                }
                match receiver.recv().ok()? {
                    Ok(batch) => *current = batch.into_iter(),
                    Err(error) => return Some(Err(error)),
                }
            },
        }
    }
}

/// Overlap fixed-width sorted-record decode with the existing direct page
/// writer. The bounded channel is charged to StorageScratch, and a failed
/// reservation uses the original serial stream without changing DDL outcome.
fn build_with_prefetched_fixed_entries<I, T, R>(
    entries: I,
    prefetch: bool,
    build: impl FnOnce(PrefetchedFixedEntries<I, T>) -> Result<R, QueryError>,
) -> Result<R, QueryError>
where
    I: Iterator<Item = Result<T, QueryError>> + Send,
    T: Send,
{
    const BATCH_ROWS: usize = 8192;
    const QUEUED_BATCHES: usize = 2;
    let reservation = prefetch
        .then(|| {
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                (QUEUED_BATCHES + 2) * BATCH_ROWS * std::mem::size_of::<T>()
                    + (QUEUED_BATCHES + 2) * std::mem::size_of::<Result<Vec<T>, QueryError>>(),
            )
        })
        .flatten();
    let Some(_reservation) = reservation else {
        return build(PrefetchedFixedEntries::Direct(entries));
    };
    std::thread::scope(|scope| {
        let (sender, receiver) = std::sync::mpsc::sync_channel(QUEUED_BATCHES);
        let producer = std::thread::Builder::new()
            .name("mydb-index-merge-read".into())
            .spawn_scoped(scope, move || {
                let mut batch = Vec::with_capacity(BATCH_ROWS);
                for entry in entries {
                    let entry = match entry {
                        Ok(entry) => entry,
                        Err(error) => {
                            // Publish every earlier value before the terminal
                            // error without reserving an error-sized slot for
                            // every successful fixed-width row.
                            if !batch.is_empty() && sender.send(Ok(batch)).is_err() {
                                return;
                            }
                            let _ = sender.send(Err(error));
                            return;
                        }
                    };
                    batch.push(entry);
                    if batch.len() == BATCH_ROWS {
                        if sender
                            .send(Ok(std::mem::replace(
                                &mut batch,
                                Vec::with_capacity(BATCH_ROWS),
                            )))
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                if !batch.is_empty() {
                    let _ = sender.send(Ok(batch));
                }
            })
            .map_err(|err| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("failed to spawn index merge reader: {err}"),
                )
            })?;
        let result = build(PrefetchedFixedEntries::Buffered {
            receiver,
            current: Vec::new().into_iter(),
        });
        producer.join().map_err(|_| {
            error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "index merge reader panicked",
            )
        })?;
        result
    })
}

impl Iterator for ValidatedFixed128Iter {
    type Item = Result<[u8; 16], QueryError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let encoded = match self.records.next()? {
            Ok(encoded) => encoded,
            Err(err) => return Some(Err(err)),
        };
        let bytes = encoded.to_be_bytes();
        if self.primary || self.unique {
            let key = &bytes[..self.key_width];
            let has_null = self
                .program
                .fixed_ops
                .as_ref()
                .is_some_and(|ops| fixed_order_key_has_null(key, ops));
            if self.primary && has_null {
                return Some(Err(error(
                    crate::error::catalog::ER_INVALID_USE_OF_NULL,
                    "invalid use of NULL value for primary key",
                )));
            }
            let key_identity = encoded >> ((16 - self.key_width) * 8);
            if self.unique && !has_null && self.previous_key == Some(key_identity) {
                return Some(Err(error(
                    crate::error::catalog::ER_DUP_ENTRY,
                    format!("duplicate entry for key '{}'", self.index_name),
                )));
            }
            if self.unique && !has_null {
                self.previous_key = Some(key_identity);
            }
        }
        debug_assert!(self.stride <= 16);
        Some(Ok(bytes))
    }
}

fn validated_fixed128_from_sorted(
    sorted: EncodedSortedRecords,
    program: Arc<CompiledIndexKeyProgram>,
    index: &IndexSchema,
) -> Result<ValidatedFixed128Iter, QueryError> {
    let (records, key_width, stride) = match sorted {
        EncodedSortedRecords::Fixed128 {
            key_width,
            stride,
            records,
        } => (
            Fixed128Source::Resident(records.into_iter()),
            key_width,
            stride,
        ),
        EncodedSortedRecords::Spilled {
            tmp,
            runs,
            fixed128: Some((key_width, stride)),
        } => (
            Fixed128Source::Runs(EncodedFixed128RunMerger::open(tmp, runs)?),
            key_width,
            stride,
        ),
        _ => unreachable!("fixed128 source was checked before conversion"),
    };
    Ok(ValidatedFixed128Iter {
        records,
        key_width,
        stride,
        program,
        unique: index.unique,
        primary: index.primary,
        index_name: index.name.clone(),
        previous_key: None,
    })
}

impl Iterator for ValidatedEncodedIndexIter {
    type Item = Result<IndexEntry, QueryError>;

    fn next(&mut self) -> Option<Self::Item> {
        let record = match self.inner.next()? {
            Ok(record) => record,
            Err(err) => return Some(Err(err)),
        };
        Some(self.program.into_index_entry(record))
    }
}

impl Iterator for ValidatedEncodedRecordIter {
    type Item = Result<EncodedIndexRecord, QueryError>;

    fn next(&mut self) -> Option<Self::Item> {
        let record = match self.inner.next()? {
            Ok(record) => record,
            Err(err) => return Some(Err(err)),
        };
        if self.primary && record.has_null {
            return Some(Err(error(
                crate::error::catalog::ER_INVALID_USE_OF_NULL,
                "invalid use of NULL value for primary key",
            )));
        }
        if self.unique
            && !record.has_null
            && self
                .previous
                .as_ref()
                .is_some_and(|key| key.as_slice() == record.order_key)
        {
            return Some(Err(error(
                crate::error::catalog::ER_DUP_ENTRY,
                format!("duplicate entry for key '{}'", self.index_name),
            )));
        }
        if self.unique && !record.has_null {
            self.previous = Some(record.order_key.clone());
        }
        Some(Ok(record))
    }
}

fn validated_encoded_record_iter(
    sorted: EncodedSortedRecords,
    program: Arc<CompiledIndexKeyProgram>,
    index: &IndexSchema,
) -> Result<ValidatedEncodedRecordIter, QueryError> {
    Ok(ValidatedEncodedRecordIter {
        inner: sorted.into_record_iter(program)?,
        unique: index.unique,
        primary: index.primary,
        index_name: index.name.clone(),
        previous: None,
    })
}

fn validated_encoded_index_iter(
    sorted: EncodedSortedRecords,
    program: Arc<CompiledIndexKeyProgram>,
    index: &IndexSchema,
) -> Result<ValidatedEncodedIndexStream, QueryError> {
    Ok(match sorted {
        EncodedSortedRecords::Fixed128 {
            key_width,
            stride,
            records,
        } => ValidatedEncodedIndexStream::Fixed128(ValidatedFixed128IndexIter {
            records: records.into_iter(),
            key_width,
            stride,
            program,
            unique: index.unique,
            primary: index.primary,
            index_name: index.name.clone(),
            previous_key: None,
        }),
        EncodedSortedRecords::Fixed {
            key_width,
            stride,
            bytes,
            order,
        } => ValidatedEncodedIndexStream::Fixed(ValidatedFixedIndexIter {
            bytes,
            order: order.into_iter(),
            key_width,
            stride,
            program,
            unique: index.unique,
            primary: index.primary,
            index_name: index.name.clone(),
            previous_ordinal: None,
        }),
        sorted => ValidatedEncodedIndexStream::Other(ValidatedEncodedIndexIter {
            inner: validated_encoded_record_iter(sorted, program.clone(), index)?,
            program,
        }),
    })
}

/// Counts key groups while consuming the sorted build stream. Online builds
/// defer rejection until captured writes have been replayed: a concurrent
/// delete may repair a duplicate or NULL present in the physical scan.
#[derive(Default)]
struct IndexBuildValidation {
    null_key_groups: u64,
    duplicate_key_groups: u64,
    previous: Option<IndexKey>,
    repeated: bool,
}

impl IndexBuildValidation {
    fn observe(&mut self, key: &IndexKey) {
        let null = key.0.iter().any(|part| matches!(part, KeyPart::Null));
        if self.previous.as_ref() == Some(key) {
            if !null && !self.repeated {
                self.duplicate_key_groups += 1;
            }
            self.repeated = true;
        } else {
            if null {
                self.null_key_groups += 1;
            }
            self.previous = Some(key.clone());
            self.repeated = false;
        }
    }
}

/// Keep enough tasks for load balancing even when the heap is smaller than
/// the old fixed 4096-page (64 MiB) task. Bound task size to retain sequential
/// I/O, and never create more scan workers than useful page ranges.
fn encoded_scan_task_pages(page_count: u64, degree: usize) -> u64 {
    page_count
        .div_ceil((degree.max(1) as u64).saturating_mul(4))
        .clamp(32, 4096)
}

/// Transcode the fixed order representation into the existing tagged page
/// representation, without constructing or cloning any KeyParts.
fn encode_fixed_leaf_key(
    bytes: &[u8],
    ops: &[FixedKeyOp],
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    out.clear();
    let count = u8::try_from(ops.len()).map_err(|_| {
        error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index key has too many parts",
        )
    })?;
    out.push(count);
    if bytes.len() != ops.iter().map(|op| op.width()).sum::<usize>() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "encoded index order key has invalid width",
        ));
    }
    let mut offset = 0;
    for &op in ops {
        let part = &bytes[offset..offset + op.width()];
        offset += op.width();
        if part[0] == 0 {
            out.push(0);
            continue;
        }
        let data = &part[1..];
        match op {
            FixedKeyOp::Signed(width) => {
                out.push(1);
                out.extend_from_slice(&decode_fixed_signed(part, width).to_le_bytes());
            }
            FixedKeyOp::Unsigned(width) => {
                let mut full = [0; 8];
                full[8 - usize::from(width)..].copy_from_slice(data);
                out.push(2);
                out.extend_from_slice(&u64::from_be_bytes(full).to_le_bytes());
            }
            FixedKeyOp::Float32 => {
                out.push(6);
                out.extend(data.iter().rev());
            }
            FixedKeyOp::Float64 => {
                out.push(7);
                out.extend(data.iter().rev());
            }
            FixedKeyOp::DecimalI128 { scale } => {
                out.push(8);
                let value = u128::from_be_bytes(data[..16].try_into().unwrap()) ^ (1_u128 << 127);
                out.extend_from_slice(&value.to_le_bytes());
                out.push(scale);
            }
            FixedKeyOp::DecimalWide { scale } => {
                let mut signed: [u8; 28] = data[..28].try_into().unwrap();
                signed[0] ^= 0x80;
                signed.reverse();
                let mut len = signed.len();
                while len > 1
                    && ((signed[len - 1] == 0 && signed[len - 2] & 0x80 == 0)
                        || (signed[len - 1] == 0xff && signed[len - 2] & 0x80 != 0))
                {
                    len -= 1;
                }
                out.extend_from_slice(&[14, len as u8]);
                out.extend_from_slice(&signed[..len]);
                out.push(scale);
            }
            FixedKeyOp::Date => {
                out.push(9);
                let value = u32::from_be_bytes(data.try_into().unwrap()) ^ (1_u32 << 31);
                out.extend_from_slice(&value.to_le_bytes());
            }
            FixedKeyOp::Time | FixedKeyOp::DateTime => {
                out.push(if matches!(op, FixedKeyOp::Time) {
                    10
                } else {
                    11
                });
                let value = u64::from_be_bytes(data.try_into().unwrap()) ^ (1_u64 << 63);
                out.extend_from_slice(&value.to_le_bytes());
            }
            FixedKeyOp::Year => {
                out.push(12);
                out.push(data[0]);
            }
        }
    }
    Ok(())
}

/// Borrow records directly from resident arenas. Spilled records retain the
/// existing checked merger and its memory accounting.
fn visit_sorted_encoded_records<F>(
    sorted: EncodedSortedRecords,
    program: Arc<CompiledIndexKeyProgram>,
    mut visit: F,
) -> Result<(), QueryError>
where
    F: FnMut(&[u8], Option<&[u8]>, RowId, bool) -> Result<(), QueryError>,
{
    match sorted {
        EncodedSortedRecords::Fixed {
            key_width,
            stride,
            bytes,
            order,
        } => {
            let ops = program.fixed_ops.as_deref().unwrap();
            for (position, ordinal) in order.into_iter().enumerate() {
                if position % 4096 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                let record = &bytes[ordinal as usize * stride..][..stride];
                let key = &record[..key_width];
                let row_id = RowId {
                    page_id: u32::from_be_bytes(
                        record[key_width..key_width + 4].try_into().unwrap(),
                    ),
                    slot_id: u16::from_be_bytes(record[key_width + 4..].try_into().unwrap()),
                };
                visit(key, None, row_id, fixed_order_key_has_null(key, ops))?;
            }
        }
        EncodedSortedRecords::Fixed128 {
            key_width, records, ..
        } => {
            let ops = program.fixed_ops.as_deref().unwrap();
            for (position, record) in records.into_iter().enumerate() {
                if position % 4096 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                let bytes = record.to_be_bytes();
                let key = &bytes[..key_width];
                visit(
                    key,
                    None,
                    decode_fixed128_row_id(&bytes, key_width),
                    fixed_order_key_has_null(key, ops),
                )?;
            }
        }
        EncodedSortedRecords::Spilled {
            tmp,
            runs,
            fixed128: Some((key_width, _)),
        } => {
            let ops = program.fixed_ops.as_deref().unwrap();
            for (position, record) in EncodedFixed128RunMerger::open(tmp, runs)?.enumerate() {
                if position % 4096 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                let bytes = record?.to_be_bytes();
                let key = &bytes[..key_width];
                visit(
                    key,
                    None,
                    decode_fixed128_row_id(&bytes, key_width),
                    fixed_order_key_has_null(key, ops),
                )?;
            }
        }
        EncodedSortedRecords::Variable { arena, records } => {
            for (position, record) in records.into_iter().enumerate() {
                if position % 4096 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                let key = &arena[record.order_start as usize..][..record.order_len as usize];
                let payload =
                    &arena[record.payload_start as usize..][..record.payload_len as usize];
                visit(
                    key,
                    (!payload.is_empty()).then_some(payload),
                    record.row_id,
                    record.has_null,
                )?;
            }
        }
        sorted => {
            for (position, record) in sorted.into_record_iter(program)?.enumerate() {
                if position % 4096 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                let record = record?;
                visit(
                    &record.order_key,
                    record.materialized_key.as_deref(),
                    record.row_id,
                    record.has_null,
                )?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
fn create_rebuilt_from_encoded_composite(
    db: &Database,
    path: PathBuf,
    index: &IndexSchema,
    program: Arc<CompiledIndexKeyProgram>,
    sorted: EncodedSortedRecords,
    validation: &mut Option<IndexBuildValidation>,
) -> Result<IndexFile, QueryError> {
    create_rebuilt_from_encoded_composite_with_degree(
        db, path, index, program, sorted, validation, 1,
    )
}

fn create_rebuilt_from_encoded_composite_with_degree(
    db: &Database,
    path: PathBuf,
    index: &IndexSchema,
    program: Arc<CompiledIndexKeyProgram>,
    sorted: EncodedSortedRecords,
    validation: &mut Option<IndexBuildValidation>,
    degree: usize,
) -> Result<IndexFile, QueryError> {
    let mut page_key = Vec::new();
    let mut previous = Vec::new();
    let mut have_previous = false;
    let mut repeated = false;
    let check_groups = index.unique || index.primary || validation.is_some();
    IndexFile::create_rebuilt_from_encoded_keys(
        path,
        |append| {
            visit_sorted_encoded_records(
                sorted,
                program.clone(),
                |order, payload, row_id, has_null| {
                    if check_groups {
                        let equal = have_previous && previous == order;
                        if index.primary && has_null {
                            return Err(error(
                                crate::error::catalog::ER_INVALID_USE_OF_NULL,
                                "invalid use of NULL value for primary key",
                            ));
                        }
                        if index.unique && !has_null && equal {
                            return Err(error(
                                crate::error::catalog::ER_DUP_ENTRY,
                                format!("duplicate entry for key '{}'", index.name),
                            ));
                        }
                        if let Some(counts) = validation.as_mut() {
                            if equal {
                                if !has_null && !repeated {
                                    counts.duplicate_key_groups += 1;
                                }
                            } else if has_null {
                                counts.null_key_groups += 1;
                            }
                        }
                        repeated = equal;
                        if !equal {
                            previous.clear();
                            previous.extend_from_slice(order);
                            have_previous = true;
                        }
                    }
                    if let Some(payload) = payload {
                        let len = encoded_materialized_leaf_key_len(payload)?;
                        page_key.clear();
                        page_key.reserve(len);
                        page_key.push(u32::from_le_bytes(payload[..4].try_into().unwrap()) as u8);
                        page_key.extend_from_slice(&payload[4..]);
                    } else if let Some(ops) = program.fixed_ops.as_deref() {
                        encode_fixed_leaf_key(order, ops, &mut page_key)?;
                    } else {
                        // Payload elision is legal when every actual part is
                        // reconstructible from its order key (including NULL
                        // in a declared string column and numeric expressions).
                        let key = decode_key_memcmp(order, &program.decode_schema).map_err(
                            |message| {
                                error(
                                    crate::error::catalog::ER_GET_ERRNO,
                                    format!("invalid encoded index order key: {message}"),
                                )
                            },
                        )?;
                        u8::try_from(key.0.len()).map_err(|_| {
                            error(
                                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                                "index key has too many parts",
                            )
                        })?;
                        page_key.clear();
                        encode_index_key_into(&key, &mut page_key)?;
                        // The canonical stream count is u32; the page count
                        // is u8. Tagged parts have an identical representation.
                        page_key.drain(1..4);
                    }
                    append(&page_key, row_id)
                },
            )
        },
        db.log.clone(),
        db.config.durability,
        db.buffer_pool.clone(),
        degree,
    )
}

fn create_rebuilt_from_encoded_sorted(
    db: &Database,
    path: PathBuf,
    index: &IndexSchema,
    program: Arc<CompiledIndexKeyProgram>,
    sorted: EncodedSortedRecords,
    validation: &mut Option<IndexBuildValidation>,
) -> Result<IndexFile, QueryError> {
    create_rebuilt_from_encoded_sorted_with_degree(db, path, index, program, sorted, validation, 1)
}

fn create_rebuilt_from_encoded_sorted_with_degree(
    db: &Database,
    path: PathBuf,
    index: &IndexSchema,
    program: Arc<CompiledIndexKeyProgram>,
    sorted: EncodedSortedRecords,
    validation: &mut Option<IndexBuildValidation>,
    degree: usize,
) -> Result<IndexFile, QueryError> {
    // Three or more parts cannot use any existing compact leaf layout.
    // Keep narrow and interned layouts on their existing dense writers.
    if db.config.durability != DurabilityMode::Memory
        && (index.is_fulltext()
            || program.index.columns.len() + program.index.included_columns.len() >= 3)
        && !matches!(&sorted, EncodedSortedRecords::Interned { .. })
    {
        return create_rebuilt_from_encoded_composite_with_degree(
            db, path, index, program, sorted, validation, degree,
        );
    }
    if program.physical_lane != EncodedPhysicalLane::Generic
        || matches!(&sorted, EncodedSortedRecords::Interned { .. })
    {
        *validation = None;
    }
    let sorted = match sorted {
        EncodedSortedRecords::Interned { mut keys, records } => {
            for key in &mut keys {
                if key.materialized_key.is_some() {
                    continue;
                }
                let record = EncodedIndexRecord {
                    order_key: key.order_key.clone(),
                    materialized_key: None,
                    row_id: RowId {
                        page_id: 0,
                        slot_id: 0,
                    },
                    has_null: key.has_null,
                };
                let materialized = program.materialize_key(&record)?;
                let mut payload = Vec::new();
                encode_index_key_into(&materialized, &mut payload)?;
                key.materialized_key = Some(payload);
            }
            return IndexFile::create_rebuilt_from_sorted_interned_with_durability(
                path,
                keys,
                records,
                index,
                db.log.clone(),
                db.config.durability,
                db.buffer_pool.clone(),
                degree,
            );
        }
        sorted => sorted,
    };
    match program.physical_lane {
        EncodedPhysicalLane::SingleInt => {
            if matches!(
                &sorted,
                EncodedSortedRecords::Fixed128 { .. }
                    | EncodedSortedRecords::Spilled {
                        fixed128: Some(_),
                        ..
                    }
            ) {
                let prefetch = fixed128_merge_prefetch(&sorted);
                let [FixedKeyOp::Signed(count)] = program.fixed_ops.as_deref().unwrap() else {
                    unreachable!()
                };
                let count = *count;
                let source = validated_fixed128_from_sorted(sorted, program.clone(), index)?;
                let key_width = source.key_width;
                let stream = source.map(move |record| {
                    let record = record?;
                    Ok((
                        decode_fixed_signed(&record[..key_width], count),
                        decode_fixed128_row_id(&record, key_width),
                    ))
                });
                return build_with_prefetched_fixed_entries(stream, prefetch, |stream| {
                    IndexFile::create_rebuilt_from_sorted_single_int_stream_with_durability(
                        path,
                        stream,
                        db.log.clone(),
                        db.config.durability,
                        db.buffer_pool.clone(),
                    )
                });
            }
            let source = validated_encoded_record_iter(sorted, program.clone(), index)?;
            let stream = source.map(move |record| {
                let record = record?;
                let [FixedKeyOp::Signed(bytes)] = program.fixed_ops.as_deref().unwrap() else {
                    unreachable!()
                };
                Ok((
                    decode_fixed_signed(&record.order_key, *bytes),
                    record.row_id,
                ))
            });
            IndexFile::create_rebuilt_from_sorted_single_int_stream_with_durability(
                path,
                stream,
                db.log.clone(),
                db.config.durability,
                db.buffer_pool.clone(),
            )
        }
        EncodedPhysicalLane::PairInt => {
            if matches!(
                &sorted,
                EncodedSortedRecords::Fixed128 { .. }
                    | EncodedSortedRecords::Spilled {
                        fixed128: Some(_),
                        ..
                    }
            ) {
                let prefetch = fixed128_merge_prefetch(&sorted);
                let [FixedKeyOp::Signed(left_bytes), FixedKeyOp::Signed(right_bytes)] =
                    program.fixed_ops.as_deref().unwrap()
                else {
                    unreachable!()
                };
                let left_bytes = *left_bytes;
                let right_bytes = *right_bytes;
                let left_width = 1 + usize::from(left_bytes);
                let source = validated_fixed128_from_sorted(sorted, program.clone(), index)?;
                let key_width = source.key_width;
                let stream = source.map(move |record| {
                    let record = record?;
                    Ok((
                        (
                            decode_fixed_signed(&record[..left_width], left_bytes),
                            decode_fixed_signed(&record[left_width..key_width], right_bytes),
                        ),
                        decode_fixed128_row_id(&record, key_width),
                    ))
                });
                return build_with_prefetched_fixed_entries(stream, prefetch, |stream| {
                    IndexFile::create_rebuilt_from_sorted_pair_int_stream_with_durability(
                        path,
                        stream,
                        db.log.clone(),
                        db.config.durability,
                        db.buffer_pool.clone(),
                    )
                });
            }
            let source = validated_encoded_record_iter(sorted, program.clone(), index)?;
            let stream = source.map(move |record| {
                let record = record?;
                let [FixedKeyOp::Signed(left_bytes), FixedKeyOp::Signed(right_bytes)] =
                    program.fixed_ops.as_deref().unwrap()
                else {
                    unreachable!()
                };
                let left_width = 1 + usize::from(*left_bytes);
                Ok((
                    (
                        decode_fixed_signed(&record.order_key[..left_width], *left_bytes),
                        decode_fixed_signed(&record.order_key[left_width..], *right_bytes),
                    ),
                    record.row_id,
                ))
            });
            IndexFile::create_rebuilt_from_sorted_pair_int_stream_with_durability(
                path,
                stream,
                db.log.clone(),
                db.config.durability,
                db.buffer_pool.clone(),
            )
        }
        EncodedPhysicalLane::Date => {
            if matches!(
                &sorted,
                EncodedSortedRecords::Fixed128 { .. }
                    | EncodedSortedRecords::Spilled {
                        fixed128: Some(_),
                        ..
                    }
            ) {
                let prefetch = fixed128_merge_prefetch(&sorted);
                let source = validated_fixed128_from_sorted(sorted, program, index)?;
                let key_width = source.key_width;
                let stream = source.map(move |record| {
                    let record = record?;
                    let encoded = u32::from_be_bytes(record[1..5].try_into().unwrap());
                    Ok((
                        (encoded ^ (1_u32 << 31)) as i32,
                        decode_fixed128_row_id(&record, key_width),
                    ))
                });
                return build_with_prefetched_fixed_entries(stream, prefetch, |stream| {
                    IndexFile::create_rebuilt_from_sorted_date_stream_with_durability(
                        path,
                        stream,
                        db.log.clone(),
                        db.config.durability,
                        db.buffer_pool.clone(),
                    )
                });
            }
            let source = validated_encoded_record_iter(sorted, program, index)?;
            let stream = source.map(|record| {
                let record = record?;
                let encoded = u32::from_be_bytes(record.order_key[1..5].try_into().unwrap());
                Ok(((encoded ^ (1_u32 << 31)) as i32, record.row_id))
            });
            IndexFile::create_rebuilt_from_sorted_date_stream_with_durability(
                path,
                stream,
                db.log.clone(),
                db.config.durability,
                db.buffer_pool.clone(),
            )
        }
        EncodedPhysicalLane::Generic => {
            let stream = validated_encoded_index_iter(sorted, program, index)?;
            if let Some(validation) = validation.as_mut() {
                let stream = stream.map(|entry| {
                    let entry = entry?;
                    validation.observe(&entry.key);
                    Ok(entry)
                });
                return IndexFile::create_rebuilt_from_sorted_stream_with_durability(
                    path,
                    stream,
                    db.log.clone(),
                    db.config.durability,
                    db.buffer_pool.clone(),
                );
            }
            IndexFile::create_rebuilt_from_sorted_stream_with_durability(
                path,
                stream,
                db.log.clone(),
                db.config.durability,
                db.buffer_pool.clone(),
            )
        }
    }
}

/// Spilled merges and large resident runs decode on a producer thread so the
/// run merge overlaps leaf encoding and page writes.
fn fixed128_merge_prefetch(sorted: &EncodedSortedRecords) -> bool {
    match sorted {
        EncodedSortedRecords::Spilled { .. } => true,
        EncodedSortedRecords::Fixed128 { records, .. } => records.len() >= 256 * 1024,
        _ => false,
    }
}

fn bulk_load_prelogged_encoded_sorted(
    file: &IndexFile,
    txn_id: u64,
    last_lsn: Option<u64>,
    index: &IndexSchema,
    program: Arc<CompiledIndexKeyProgram>,
    sorted: EncodedSortedRecords,
) -> Result<(), QueryError> {
    match program.physical_lane {
        EncodedPhysicalLane::SingleInt => {
            if matches!(
                &sorted,
                EncodedSortedRecords::Fixed128 { .. }
                    | EncodedSortedRecords::Spilled {
                        fixed128: Some(_),
                        ..
                    }
            ) {
                let [FixedKeyOp::Signed(count)] = program.fixed_ops.as_deref().unwrap() else {
                    unreachable!()
                };
                let count = *count;
                let prefetch = fixed128_merge_prefetch(&sorted);
                let source = validated_fixed128_from_sorted(sorted, program.clone(), index)?;
                let key_width = source.key_width;
                let stream = source.map(move |record| {
                    let record = record?;
                    Ok((
                        decode_fixed_signed(&record[..key_width], count),
                        decode_fixed128_row_id(&record, key_width),
                    ))
                });
                return build_with_prefetched_fixed_entries(stream, prefetch, |stream| {
                    file.bulk_load_prelogged_sorted_single_int_stream(txn_id, last_lsn, stream)
                });
            }
            let source = validated_encoded_record_iter(sorted, program.clone(), index)?;
            let stream = source.map(move |record| {
                let record = record?;
                let [FixedKeyOp::Signed(bytes)] = program.fixed_ops.as_deref().unwrap() else {
                    unreachable!()
                };
                Ok((
                    decode_fixed_signed(&record.order_key, *bytes),
                    record.row_id,
                ))
            });
            file.bulk_load_prelogged_sorted_single_int_stream(txn_id, last_lsn, stream)
        }
        EncodedPhysicalLane::PairInt => {
            if matches!(
                &sorted,
                EncodedSortedRecords::Fixed128 { .. }
                    | EncodedSortedRecords::Spilled {
                        fixed128: Some(_),
                        ..
                    }
            ) {
                let [FixedKeyOp::Signed(left), FixedKeyOp::Signed(right)] =
                    program.fixed_ops.as_deref().unwrap()
                else {
                    unreachable!()
                };
                let left = *left;
                let right = *right;
                let left_width = 1 + usize::from(left);
                let prefetch = fixed128_merge_prefetch(&sorted);
                let source = validated_fixed128_from_sorted(sorted, program.clone(), index)?;
                let key_width = source.key_width;
                let stream = source.map(move |record| {
                    let record = record?;
                    Ok((
                        (
                            decode_fixed_signed(&record[..left_width], left),
                            decode_fixed_signed(&record[left_width..key_width], right),
                        ),
                        decode_fixed128_row_id(&record, key_width),
                    ))
                });
                return build_with_prefetched_fixed_entries(stream, prefetch, |stream| {
                    file.bulk_load_prelogged_sorted_pair_int_stream(txn_id, last_lsn, stream)
                });
            }
            let source = validated_encoded_record_iter(sorted, program.clone(), index)?;
            let stream = source.map(move |record| {
                let record = record?;
                let [FixedKeyOp::Signed(left), FixedKeyOp::Signed(right)] =
                    program.fixed_ops.as_deref().unwrap()
                else {
                    unreachable!()
                };
                let left_width = 1 + usize::from(*left);
                Ok((
                    (
                        decode_fixed_signed(&record.order_key[..left_width], *left),
                        decode_fixed_signed(&record.order_key[left_width..], *right),
                    ),
                    record.row_id,
                ))
            });
            file.bulk_load_prelogged_sorted_pair_int_stream(txn_id, last_lsn, stream)
        }
        EncodedPhysicalLane::Date => {
            if matches!(
                &sorted,
                EncodedSortedRecords::Fixed128 { .. }
                    | EncodedSortedRecords::Spilled {
                        fixed128: Some(_),
                        ..
                    }
            ) {
                let prefetch = fixed128_merge_prefetch(&sorted);
                let source = validated_fixed128_from_sorted(sorted, program, index)?;
                let key_width = source.key_width;
                let stream = source.map(move |record| {
                    let record = record?;
                    let value = u32::from_be_bytes(record[1..5].try_into().unwrap());
                    Ok((
                        (value ^ (1_u32 << 31)) as i32,
                        decode_fixed128_row_id(&record, key_width),
                    ))
                });
                return build_with_prefetched_fixed_entries(stream, prefetch, |stream| {
                    file.bulk_load_prelogged_sorted_date_stream(txn_id, last_lsn, stream)
                });
            }
            let source = validated_encoded_record_iter(sorted, program, index)?;
            let stream = source.map(|record| {
                let record = record?;
                let value = u32::from_be_bytes(record.order_key[1..5].try_into().unwrap());
                Ok(((value ^ (1_u32 << 31)) as i32, record.row_id))
            });
            file.bulk_load_prelogged_sorted_date_stream(txn_id, last_lsn, stream)
        }
        EncodedPhysicalLane::Generic => {
            let stream = validated_encoded_index_iter(sorted, program, index)?;
            file.bulk_load_prelogged_sorted_stream(txn_id, last_lsn, stream)
        }
    }
}

fn decode_fixed128_row_id(record: &[u8; 16], key_width: usize) -> RowId {
    RowId {
        page_id: u32::from_be_bytes(record[key_width..key_width + 4].try_into().unwrap()),
        slot_id: u16::from_be_bytes(record[key_width + 4..key_width + 6].try_into().unwrap()),
    }
}

fn decode_fixed_signed(bytes: &[u8], count: u8) -> i64 {
    debug_assert_eq!(bytes[0], 1);
    let mut full = [0u8; 16];
    full[16 - usize::from(count)..].copy_from_slice(&bytes[1..]);
    let biased = u128::from_be_bytes(full) as i128;
    let bits = u32::from(count) * 8;
    (biased - (1_i128 << (bits - 1))) as i64
}

struct EncodedFixedMemoryIter {
    program: Arc<CompiledIndexKeyProgram>,
    key_width: usize,
    stride: usize,
    bytes: Vec<u8>,
    order: std::vec::IntoIter<u32>,
}

struct EncodedVariableMemoryIter {
    arena: Vec<u8>,
    records: std::vec::IntoIter<EncodedVarDescriptor>,
}

struct EncodedRunReader {
    reader: std::io::BufReader<File>,
}

struct EncodedRunMerger {
    _tmp: IndexSortTmpDir,
    readers: Vec<EncodedRunReader>,
    tournament: RunHeadTournament<EncodedIndexRecord>,
    previous: Option<EncodedIndexRecord>,
}

struct EncodedFixed128RunReader {
    reader: std::io::BufReader<File>,
}

struct EncodedFixed128RunMerger {
    _tmp: IndexSortTmpDir,
    readers: Vec<EncodedFixed128RunReader>,
    tournament: RunHeadTournament<u128>,
    previous: Option<u128>,
}

impl EncodedSortedRecords {
    fn into_record_iter(
        self,
        program: Arc<CompiledIndexKeyProgram>,
    ) -> Result<EncodedSortedRecordIter, QueryError> {
        Ok(match self {
            Self::Fixed128 {
                key_width,
                stride,
                records,
            } => EncodedSortedRecordIter::Fixed128(EncodedFixed128MemoryIter {
                program,
                key_width,
                stride,
                records: records.into_iter(),
            }),
            Self::Fixed {
                key_width,
                stride,
                bytes,
                order,
            } => EncodedSortedRecordIter::Fixed(EncodedFixedMemoryIter {
                program,
                key_width,
                stride,
                bytes,
                order: order.into_iter(),
            }),
            Self::Variable { arena, records } => {
                EncodedSortedRecordIter::Variable(EncodedVariableMemoryIter {
                    arena,
                    records: records.into_iter(),
                })
            }
            Self::Interned { keys, records } => {
                EncodedSortedRecordIter::Interned(EncodedInternedMemoryIter {
                    keys: Arc::from(keys),
                    records: records.into_iter(),
                })
            }
            Self::Spilled {
                tmp,
                runs,
                fixed128: Some((key_width, _)),
            } => EncodedSortedRecordIter::Fixed128Spilled(EncodedFixed128RunIter {
                merger: EncodedFixed128RunMerger::open(tmp, runs)?,
                program,
                key_width,
            }),
            Self::Spilled {
                tmp,
                runs,
                fixed128: None,
            } => EncodedSortedRecordIter::Spilled(EncodedRunMerger::open(tmp, runs)?),
        })
    }
}

impl Iterator for EncodedSortedRecordIter {
    type Item = Result<EncodedIndexRecord, QueryError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Fixed128(iter) => iter.next().map(Ok),
            Self::Fixed(iter) => iter.next().map(Ok),
            Self::Variable(iter) => iter.next().map(Ok),
            Self::Interned(iter) => iter.next().map(Ok),
            Self::Spilled(iter) => iter.next(),
            Self::Fixed128Spilled(iter) => iter.next(),
        }
    }
}

impl Iterator for EncodedFixed128RunIter {
    type Item = Result<EncodedIndexRecord, QueryError>;

    fn next(&mut self) -> Option<Self::Item> {
        let encoded = match self.merger.next()? {
            Ok(encoded) => encoded,
            Err(err) => return Some(Err(err)),
        };
        let bytes = encoded.to_be_bytes();
        let order_key = bytes[..self.key_width].to_vec();
        Some(Ok(EncodedIndexRecord {
            has_null: self
                .program
                .fixed_ops
                .as_ref()
                .is_some_and(|ops| fixed_order_key_has_null(&order_key, ops)),
            order_key,
            materialized_key: None,
            row_id: decode_fixed128_row_id(&bytes, self.key_width),
        }))
    }
}

impl Iterator for EncodedFixed128MemoryIter {
    type Item = EncodedIndexRecord;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.records.next()?.to_be_bytes();
        let record = &bytes[..self.stride];
        let order_key = record[..self.key_width].to_vec();
        Some(EncodedIndexRecord {
            has_null: self
                .program
                .fixed_ops
                .as_ref()
                .is_some_and(|ops| fixed_order_key_has_null(&order_key, ops)),
            order_key,
            materialized_key: None,
            row_id: RowId {
                page_id: u32::from_be_bytes(
                    record[self.key_width..self.key_width + 4]
                        .try_into()
                        .unwrap(),
                ),
                slot_id: u16::from_be_bytes(record[self.key_width + 4..].try_into().unwrap()),
            },
        })
    }
}

impl Iterator for EncodedFixedMemoryIter {
    type Item = EncodedIndexRecord;

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.order.next()? as usize * self.stride;
        let record = self.bytes.get(start..start + self.stride)?;
        let order_key = record[..self.key_width].to_vec();
        Some(EncodedIndexRecord {
            has_null: self
                .program
                .fixed_ops
                .as_ref()
                .is_some_and(|ops| fixed_order_key_has_null(&order_key, ops)),
            order_key,
            materialized_key: None,
            row_id: RowId {
                page_id: u32::from_be_bytes(
                    record[self.key_width..self.key_width + 4]
                        .try_into()
                        .unwrap(),
                ),
                slot_id: u16::from_be_bytes(record[self.key_width + 4..].try_into().unwrap()),
            },
        })
    }
}

impl Iterator for EncodedVariableMemoryIter {
    type Item = EncodedIndexRecord;

    fn next(&mut self) -> Option<Self::Item> {
        let record = self.records.next()?;
        let payload = descriptor_materialized_key(&self.arena, &record);
        Some(EncodedIndexRecord {
            order_key: descriptor_order_key(&self.arena, &record).to_vec(),
            materialized_key: (!payload.is_empty()).then(|| payload.to_vec()),
            row_id: record.row_id,
            has_null: record.has_null,
        })
    }
}

impl Iterator for EncodedInternedMemoryIter {
    type Item = EncodedIndexRecord;

    fn next(&mut self) -> Option<Self::Item> {
        let record = self.records.next()?;
        let key = self.keys.get(interned_record_key_id(record) as usize)?;
        Some(EncodedIndexRecord {
            order_key: key.order_key.clone(),
            materialized_key: key.materialized_key.clone(),
            row_id: interned_record_row_id(record),
            has_null: key.has_null,
        })
    }
}

impl EncodedRunReader {
    fn open(path: &Path) -> Result<Self, QueryError> {
        if let Ok(meta) = fs::metadata(path) {
            record_spill_bytes_read(meta.len());
        }
        Ok(Self {
            reader: std::io::BufReader::with_capacity(64 * 1024, File::open(path).map_err(io_err)?),
        })
    }

    fn next_record(&mut self) -> Result<Option<EncodedIndexRecord>, QueryError> {
        let mut header = [0u8; 15];
        if self.reader.read(&mut header[..1]).map_err(io_err)? == 0 {
            return Ok(None);
        }
        self.reader.read_exact(&mut header[1..]).map_err(io_err)?;
        let order_len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let payload_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let row_id = RowId {
            page_id: u32::from_le_bytes(header[8..12].try_into().unwrap()),
            slot_id: u16::from_le_bytes(header[12..14].try_into().unwrap()),
        };
        let mut order_key = vec![0u8; order_len];
        self.reader.read_exact(&mut order_key).map_err(io_err)?;
        let materialized_key = if payload_len == 0 {
            None
        } else {
            let mut payload = vec![0u8; payload_len];
            self.reader.read_exact(&mut payload).map_err(io_err)?;
            Some(payload)
        };
        Ok(Some(EncodedIndexRecord {
            order_key,
            materialized_key,
            row_id,
            has_null: header[14] != 0,
        }))
    }
}

struct EncodedIndexRunCodec;

impl IndexSortRunCodec for EncodedIndexRunCodec {
    type Entry = EncodedIndexRecord;
    type Reader = EncodedRunReader;
    type Writer = std::io::BufWriter<File>;

    fn open_reader(path: &Path) -> Result<Self::Reader, QueryError> {
        Ok(EncodedRunReader {
            reader: std::io::BufReader::with_capacity(64 * 1024, File::open(path).map_err(io_err)?),
        })
    }

    fn next_entry(reader: &mut Self::Reader) -> Result<Option<Self::Entry>, QueryError> {
        reader.next_record()
    }

    fn create_writer(path: &Path) -> Result<Self::Writer, QueryError> {
        Ok(std::io::BufWriter::with_capacity(
            64 * 1024,
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .map_err(io_err)?,
        ))
    }

    fn append(writer: &mut Self::Writer, entry: &Self::Entry) -> Result<(), QueryError> {
        write_encoded_run_record(
            writer,
            &entry.order_key,
            entry.materialized_key.as_deref(),
            entry.row_id,
            entry.has_null,
        )
    }

    fn finish(mut writer: Self::Writer) -> Result<(), QueryError> {
        writer.flush().map_err(io_err)
    }
}

impl EncodedRunMerger {
    fn open(tmp: IndexSortTmpDir, runs: Vec<PathBuf>) -> Result<Self, QueryError> {
        // Sort arenas have been dropped before this phase. Bound both open
        // files and read buffers independently of relation size/run count.
        let runs = consolidate_sort_runs::<EncodedIndexRunCodec>(
            &tmp.path,
            runs,
            index_merge_fan_in().min(16),
        )?;
        let mut readers = Vec::with_capacity(runs.len());
        let mut heads = Vec::with_capacity(runs.len());
        for path in runs {
            let mut reader = EncodedRunReader::open(&path)?;
            let Some(record) = reader.next_record()? else {
                continue;
            };
            readers.push(reader);
            heads.push(Some(record));
        }
        Ok(Self {
            _tmp: tmp,
            readers,
            tournament: RunHeadTournament::new(heads),
            previous: None,
        })
    }
}

impl Iterator for EncodedRunMerger {
    type Item = Result<EncodedIndexRecord, QueryError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let run = self.tournament.winner()?;
            let next = match self.readers[run].next_record() {
                Ok(next) => next,
                Err(err) => return Some(Err(err)),
            };
            let record = self
                .tournament
                .replace(run, next)
                .expect("encoded run winner has live head");
            if self.previous.as_ref() == Some(&record) {
                continue;
            }
            self.previous = Some(record.clone());
            return Some(Ok(record));
        }
    }
}

impl EncodedFixed128RunReader {
    fn open(path: &Path) -> Result<Self, QueryError> {
        if let Ok(meta) = fs::metadata(path) {
            record_spill_bytes_read(meta.len());
        }
        Ok(Self {
            reader: std::io::BufReader::with_capacity(64 * 1024, File::open(path).map_err(io_err)?),
        })
    }

    fn next_record(&mut self) -> Result<Option<u128>, QueryError> {
        let mut bytes = [0u8; 16];
        if self.reader.read(&mut bytes[..1]).map_err(io_err)? == 0 {
            return Ok(None);
        }
        self.reader.read_exact(&mut bytes[1..]).map_err(io_err)?;
        Ok(Some(u128::from_be_bytes(bytes)))
    }
}

struct EncodedFixed128RunCodec;

impl IndexSortRunCodec for EncodedFixed128RunCodec {
    type Entry = u128;
    type Reader = EncodedFixed128RunReader;
    type Writer = std::io::BufWriter<File>;

    fn open_reader(path: &Path) -> Result<Self::Reader, QueryError> {
        Ok(EncodedFixed128RunReader {
            reader: std::io::BufReader::with_capacity(64 * 1024, File::open(path).map_err(io_err)?),
        })
    }

    fn next_entry(reader: &mut Self::Reader) -> Result<Option<Self::Entry>, QueryError> {
        reader.next_record()
    }

    fn create_writer(path: &Path) -> Result<Self::Writer, QueryError> {
        Ok(std::io::BufWriter::with_capacity(
            64 * 1024,
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .map_err(io_err)?,
        ))
    }

    fn append(writer: &mut Self::Writer, entry: &Self::Entry) -> Result<(), QueryError> {
        writer.write_all(&entry.to_be_bytes()).map_err(io_err)
    }

    fn finish(mut writer: Self::Writer) -> Result<(), QueryError> {
        writer.flush().map_err(io_err)
    }
}

impl EncodedFixed128RunMerger {
    fn open(tmp: IndexSortTmpDir, runs: Vec<PathBuf>) -> Result<Self, QueryError> {
        let runs = consolidate_sort_runs::<EncodedFixed128RunCodec>(
            &tmp.path,
            runs,
            index_merge_fan_in().min(16),
        )?;
        let mut readers = Vec::with_capacity(runs.len());
        let mut heads = Vec::with_capacity(runs.len());
        for path in runs {
            let mut reader = EncodedFixed128RunReader::open(&path)?;
            if let Some(record) = reader.next_record()? {
                readers.push(reader);
                heads.push(Some(record));
            }
        }
        Ok(Self {
            _tmp: tmp,
            readers,
            tournament: RunHeadTournament::new(heads),
            previous: None,
        })
    }
}

impl Iterator for EncodedFixed128RunMerger {
    type Item = Result<u128, QueryError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let run = self.tournament.winner()?;
            let next = match self.readers[run].next_record() {
                Ok(next) => next,
                Err(err) => return Some(Err(err)),
            };
            let record = self
                .tournament
                .replace(run, next)
                .expect("fixed encoded run winner has live head");
            if self.previous == Some(record) {
                continue;
            }
            self.previous = Some(record);
            return Some(Ok(record));
        }
    }
}

impl EncodedIndexSorter {
    fn new(program: Arc<CompiledIndexKeyProgram>, run_bytes: usize) -> Self {
        let run_bytes = run_bytes.max(1024 * 1024);
        let storage = if let Some(record_width) = program.fixed_record_width() {
            let key_width = record_width - 6;
            if record_width <= 16 {
                // Rank-partition the u128 records in place before sorting
                // disjoint slices. No second record-sized merge vector is
                // needed, so the whole governed envelope stores input rows.
                EncodedSortStorage::Fixed128 {
                    key_width,
                    stride: record_width,
                    records: Vec::with_capacity(run_bytes / std::mem::size_of::<u128>()),
                }
            } else {
                // Wide fixed records sort immutable arena ordinals with the
                // in-place MSD path below. Capacity consists of one record
                // arena plus one u32 ordinal per row; no second arena-sized
                // radix scratch is allocated.
                let capacity = run_bytes / (record_width + std::mem::size_of::<u32>());
                EncodedSortStorage::Fixed {
                    key_width,
                    stride: record_width,
                    bytes: Vec::with_capacity(capacity * record_width),
                    order: Vec::with_capacity(capacity),
                }
            }
        } else {
            // Variable sorting temporarily duplicates the descriptor array,
            // but not the byte arena. Keep arena + source descriptors + sort
            // scratch within the StorageScratch share.
            EncodedSortStorage::Variable {
                arena: Vec::with_capacity(run_bytes.saturating_mul(2) / 3),
                records: Vec::with_capacity(
                    (run_bytes / 6 / std::mem::size_of::<EncodedVarDescriptor>()).max(1),
                ),
                dictionary_sampled: false,
            }
        };
        Self {
            program,
            storage,
            run_bytes,
            tmp: None,
            runs: Vec::new(),
            sort_degree: index_sort_parallel_degree(),
        }
    }

    fn with_sort_degree(mut self, degree: usize) -> Self {
        self.sort_degree = degree.max(1);
        self
    }

    fn intern_pending_key(
        keys: &mut Vec<EncodedInternedKey>,
        lookup: &mut EncodedInternedLookup,
        pending: EncodedPendingIndexRecord,
    ) -> Result<u32, QueryError> {
        let mut hasher = lookup.hasher().build_hasher();
        pending.order_key.hash(&mut hasher);
        pending.materialized_key.hash(&mut hasher);
        pending.has_null.hash(&mut hasher);
        let hash = hasher.finish();
        match lookup.raw_entry_mut().from_hash(hash, |key| {
            key.order_key.as_slice() == pending.order_key.as_slice()
                && key.materialized_key.as_deref() == pending.materialized_key.as_deref()
                && key.has_null == pending.has_null
        }) {
            hashbrown::hash_map::RawEntryMut::Occupied(entry) => Ok(*entry.get()),
            hashbrown::hash_map::RawEntryMut::Vacant(entry) => {
                let key_id = u32::try_from(keys.len()).map_err(|_| {
                    error(
                        crate::error::catalog::ER_OUT_OF_RESOURCES,
                        "encoded index dictionary exceeds u32 key ids",
                    )
                })?;
                let key = EncodedInternedKey {
                    order_key: pending.order_key.into_vec(),
                    materialized_key: pending.materialized_key,
                    has_null: pending.has_null,
                };
                entry.insert_hashed_nocheck(hash, key.clone(), key_id);
                keys.push(key);
                Ok(key_id)
            }
        }
    }

    /// Promote a sampled low-cardinality variable sorter into dictionary-id
    /// records. High-cardinality samples keep the existing arena/descriptor
    /// oracle unchanged.
    fn maybe_promote_variable_dictionary(&mut self) -> Result<(), QueryError> {
        self.maybe_promote_variable_dictionary_after(VARIABLE_DICTIONARY_SAMPLE_ROWS)
    }

    fn maybe_promote_variable_dictionary_after(
        &mut self,
        sample_rows: usize,
    ) -> Result<(), QueryError> {
        let should_sample = matches!(
            &self.storage,
            EncodedSortStorage::Variable {
                records,
                dictionary_sampled: false,
                ..
            } if records.len() >= sample_rows
        );
        if !should_sample {
            return Ok(());
        }

        let placeholder = EncodedSortStorage::Variable {
            arena: Vec::new(),
            records: Vec::new(),
            dictionary_sampled: true,
        };
        let EncodedSortStorage::Variable {
            arena,
            records,
            dictionary_sampled: _,
        } = std::mem::replace(&mut self.storage, placeholder)
        else {
            unreachable!("dictionary promotion shape changed")
        };

        let dictionary_budget = (self.run_bytes / 8).max(64 * 1024);
        let record_capacity =
            self.run_bytes.saturating_sub(dictionary_budget) / (2 * std::mem::size_of::<u128>());
        if record_capacity < records.len() {
            self.storage = EncodedSortStorage::Variable {
                arena,
                records,
                dictionary_sampled: true,
            };
            return Ok(());
        }

        let initial_keys = records
            .len()
            .min(VARIABLE_DICTIONARY_SAMPLE_DISTINCT_MAX)
            .min(dictionary_budget / 1024)
            .max(1);
        let mut keys = Vec::with_capacity(initial_keys);
        let mut lookup =
            hashbrown::HashMap::with_capacity_and_hasher(initial_keys, ahash::RandomState::new());
        let mut packed = Vec::with_capacity(record_capacity.max(records.len()));
        let mut dictionary_bytes = 0usize;
        for descriptor in &records {
            let pending = EncodedPendingIndexRecord {
                order_key: smallvec::SmallVec::from_slice(descriptor_order_key(&arena, descriptor)),
                materialized_key: {
                    let payload = descriptor_materialized_key(&arena, descriptor);
                    (!payload.is_empty()).then(|| payload.to_vec())
                },
                has_null: descriptor.has_null,
            };
            let prior = keys.len();
            let key_id = Self::intern_pending_key(&mut keys, &mut lookup, pending)?;
            if keys.len() != prior {
                dictionary_bytes += interned_key_capacity_bytes(&keys[key_id as usize]);
            }
            if keys.len() > VARIABLE_DICTIONARY_SAMPLE_DISTINCT_MAX
                || dictionary_bytes > dictionary_budget
            {
                self.storage = EncodedSortStorage::Variable {
                    arena,
                    records,
                    dictionary_sampled: true,
                };
                return Ok(());
            }
            packed.push(pack_interned_record(key_id, key_id, descriptor.row_id));
        }
        self.storage = EncodedSortStorage::Interned {
            dictionary_bytes,
            keys,
            lookup,
            records: packed,
            dictionary_budget,
        };
        INDEX_BUILD_DICTIONARY_BUILDS.fetch_add(1, Ordering::Relaxed);
        INDEX_BUILD_DICTIONARY_ROWS.fetch_add(records.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Append fixed records produced by a worker-local projected heap decoder.
    /// Each record is already `[order_key | RowId]`, so this path performs no
    /// `Value`, `IndexKey`, pending-record, or per-row heap allocation.
    fn push_fixed_batch(&mut self, data_dir: &Path, batch: &[u8]) -> Result<(), QueryError> {
        let stride = match &self.storage {
            EncodedSortStorage::Fixed128 { stride, .. }
            | EncodedSortStorage::Fixed { stride, .. } => *stride,
            EncodedSortStorage::Variable { .. } | EncodedSortStorage::Interned { .. } => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "fixed encoded batch was sent to a variable sorter",
                ));
            }
        };
        if batch.len() % stride != 0 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "fixed encoded batch has an invalid stride",
            ));
        }
        let mut remaining = batch;
        while !remaining.is_empty() {
            if self.len() != 0 && self.next_record_exceeds_capacity(stride, 0) {
                self.flush_run(data_dir)?;
            }
            self.ensure_next_record_fits_capacity(stride, 0)?;
            // Admit a whole prefix against the already-reserved capacity.
            // Crossing the boundary still spills before any Vec can grow.
            let count = match &self.storage {
                EncodedSortStorage::Fixed128 { records, .. } => records.capacity() - records.len(),
                EncodedSortStorage::Fixed { bytes, order, .. } => {
                    ((bytes.capacity() - bytes.len()) / stride).min(order.capacity() - order.len())
                }
                _ => unreachable!(),
            }
            .min(remaining.len() / stride);
            let (prefix, tail) = remaining.split_at(count * stride);
            match &mut self.storage {
                EncodedSortStorage::Fixed128 { records, .. } => {
                    append_fixed128_records(records, prefix, stride);
                }
                EncodedSortStorage::Fixed { bytes, order, .. } => {
                    let first = bytes.len() / stride;
                    bytes.extend_from_slice(prefix);
                    order.extend((first..first + count).map(|ordinal| ordinal as u32));
                }
                _ => unreachable!(),
            }
            remaining = tail;
        }
        Ok(())
    }

    fn push_fixed_keys_with_row_ids(
        &mut self,
        data_dir: &Path,
        keys: &[u8],
        key_width: usize,
        row_ids: &[RowId],
    ) -> Result<(), QueryError> {
        if keys.len() != row_ids.len().saturating_mul(key_width) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "fixed encoded LOAD key batch does not match inserted rows",
            ));
        }
        let stride = key_width + 6;
        for (key, row_id) in keys.chunks_exact(key_width).zip(row_ids.iter().copied()) {
            if self.len() != 0 && self.next_record_exceeds_capacity(stride, 0) {
                self.flush_run(data_dir)?;
            }
            self.ensure_next_record_fits_capacity(stride, 0)?;
            match &mut self.storage {
                EncodedSortStorage::Fixed128 {
                    key_width: expected,
                    stride: expected_stride,
                    records,
                } if *expected == key_width && *expected_stride == stride => {
                    let mut encoded = [0u8; 16];
                    encoded[..key_width].copy_from_slice(key);
                    encoded[key_width..key_width + 4]
                        .copy_from_slice(&row_id.page_id.to_be_bytes());
                    encoded[key_width + 4..stride].copy_from_slice(&row_id.slot_id.to_be_bytes());
                    records.push(u128::from_be_bytes(encoded));
                }
                EncodedSortStorage::Fixed {
                    key_width: expected,
                    stride: expected_stride,
                    bytes,
                    order,
                } if *expected == key_width && *expected_stride == stride => {
                    bytes.extend_from_slice(key);
                    bytes.extend_from_slice(&row_id.page_id.to_be_bytes());
                    bytes.extend_from_slice(&row_id.slot_id.to_be_bytes());
                    order.push((bytes.len() / stride - 1) as u32);
                }
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "fixed encoded LOAD key does not match sorter schema",
                    ));
                }
            }
        }
        Ok(())
    }

    fn push_row(
        &mut self,
        data_dir: &Path,
        table: &TableSchema,
        values: &[Value],
        row_id: RowId,
    ) -> Result<(), QueryError> {
        if self.program.fixed_ops.is_some() && self.program.direct_parts.is_some() {
            let mut record = Vec::with_capacity(
                self.program
                    .fixed_record_width()
                    .expect("fixed program exposes record width"),
            );
            self.program.encode_fixed_values_into(values, &mut record)?;
            record.extend_from_slice(&row_id.page_id.to_be_bytes());
            record.extend_from_slice(&row_id.slot_id.to_be_bytes());
            return self.push_fixed_batch(data_dir, &record);
        }
        for pending in self.program.encode_row(table, values)? {
            self.push(data_dir, pending, row_id)?;
        }
        Ok(())
    }

    fn push(
        &mut self,
        data_dir: &Path,
        pending: EncodedPendingIndexRecord,
        row_id: RowId,
    ) -> Result<(), QueryError> {
        let order_bytes = pending.order_key.len();
        let payload_bytes = pending.materialized_key.as_ref().map_or(0, Vec::len);
        if self.len() != 0 && self.next_record_exceeds_capacity(order_bytes, payload_bytes) {
            self.flush_run(data_dir)?;
        }
        self.ensure_next_record_fits_capacity(order_bytes, payload_bytes)?;
        match &mut self.storage {
            EncodedSortStorage::Fixed128 {
                key_width,
                stride,
                records,
            } => {
                if pending.materialized_key.is_some() || pending.order_key.len() != *key_width {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "fixed128 encoded sorter received variable record",
                    ));
                }
                let mut encoded = [0u8; 16];
                encoded[..*key_width].copy_from_slice(&pending.order_key);
                encoded[*key_width..*key_width + 4].copy_from_slice(&row_id.page_id.to_be_bytes());
                encoded[*key_width + 4..*stride].copy_from_slice(&row_id.slot_id.to_be_bytes());
                records.push(u128::from_be_bytes(encoded));
            }
            EncodedSortStorage::Fixed {
                key_width,
                stride,
                bytes,
                order,
            } => {
                if pending.materialized_key.is_some() || pending.order_key.len() != *key_width {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "fixed encoded sorter received variable record",
                    ));
                }
                bytes.extend_from_slice(&pending.order_key);
                bytes.extend_from_slice(&row_id.page_id.to_be_bytes());
                bytes.extend_from_slice(&row_id.slot_id.to_be_bytes());
                order.push((bytes.len() / *stride - 1) as u32);
                debug_assert_eq!(bytes.len() % *stride, 0);
            }
            EncodedSortStorage::Variable { arena, records, .. } => {
                let order_start = u32::try_from(arena.len()).map_err(encoded_arena_too_large)?;
                let order_len =
                    u32::try_from(pending.order_key.len()).map_err(encoded_arena_too_large)?;
                arena.extend_from_slice(&pending.order_key);
                let payload_start = u32::try_from(arena.len()).map_err(encoded_arena_too_large)?;
                let payload_len =
                    u32::try_from(pending.materialized_key.as_ref().map_or(0, Vec::len))
                        .map_err(encoded_arena_too_large)?;
                if let Some(payload) = pending.materialized_key {
                    arena.extend_from_slice(&payload);
                }
                records.push(EncodedVarDescriptor {
                    order_start,
                    order_len,
                    payload_start,
                    payload_len,
                    row_id,
                    has_null: pending.has_null,
                });
            }
            EncodedSortStorage::Interned {
                keys,
                lookup,
                records,
                dictionary_bytes,
                ..
            } => {
                let previous_len = keys.len();
                let key_id = Self::intern_pending_key(keys, lookup, pending)?;
                if keys.len() != previous_len {
                    *dictionary_bytes += interned_key_capacity_bytes(&keys[key_id as usize]);
                }
                records.push(pack_interned_record(key_id, key_id, row_id));
            }
        }
        self.maybe_promote_variable_dictionary()?;
        Ok(())
    }

    fn len(&self) -> usize {
        match &self.storage {
            EncodedSortStorage::Fixed128 { records, .. } => records.len(),
            EncodedSortStorage::Fixed { order, .. } => order.len(),
            EncodedSortStorage::Variable { records, .. } => records.len(),
            EncodedSortStorage::Interned { records, .. } => records.len(),
        }
    }

    /// Return whether appending one record would make any owned Vec grow.
    ///
    /// The constructor divides `run_bytes` across the resident source arena,
    /// the ordinal/descriptor array and the equally-sized sorting scratch.
    /// Testing only `len * stride` here used to let Vec's geometric growth
    /// escape that division: a wide fixed-key run could retain roughly the
    /// complete budget as source bytes and then allocate another complete
    /// budget for radix scratch. Spill at the pre-reserved capacity boundary
    /// so the reservation continues to describe the peak allocation by
    /// capacity, not merely the currently initialized length.
    fn next_record_exceeds_capacity(&self, order_bytes: usize, payload_bytes: usize) -> bool {
        match &self.storage {
            EncodedSortStorage::Fixed128 { records, .. } => records.len() == records.capacity(),
            EncodedSortStorage::Fixed {
                bytes,
                stride,
                order,
                ..
            } => {
                bytes.len().saturating_add(*stride) > bytes.capacity()
                    || order.len() == order.capacity()
            }
            EncodedSortStorage::Variable { arena, records, .. } => {
                arena
                    .len()
                    .saturating_add(order_bytes)
                    .saturating_add(payload_bytes)
                    > arena.capacity()
                    || records.len() == records.capacity()
            }
            EncodedSortStorage::Interned {
                records,
                dictionary_budget,
                dictionary_bytes,
                ..
            } => {
                records.len() == records.capacity()
                    || dictionary_bytes
                        .saturating_add(order_bytes)
                        .saturating_add(payload_bytes)
                        > *dictionary_budget
            }
        }
    }

    fn ensure_next_record_fits_capacity(
        &self,
        order_bytes: usize,
        payload_bytes: usize,
    ) -> Result<(), QueryError> {
        if !self.next_record_exceeds_capacity(order_bytes, payload_bytes) {
            return Ok(());
        }
        Err(error(
            crate::error::catalog::ER_OUTOFMEMORY,
            format!(
                "encoded index record exceeds the bounded sort run capacity ({} bytes)",
                self.run_bytes
            ),
        ))
    }

    fn capacity_bytes(&self) -> usize {
        match &self.storage {
            EncodedSortStorage::Fixed128 { records, .. } => records
                .capacity()
                .saturating_mul(std::mem::size_of::<u128>()),
            EncodedSortStorage::Fixed { bytes, order, .. } => bytes
                .capacity()
                .saturating_add(order.capacity().saturating_mul(std::mem::size_of::<u32>())),
            EncodedSortStorage::Variable { arena, records, .. } => arena.capacity().saturating_add(
                records
                    .capacity()
                    .saturating_mul(2 * std::mem::size_of::<EncodedVarDescriptor>()),
            ),
            EncodedSortStorage::Interned {
                records,
                dictionary_budget,
                ..
            } => records
                .capacity()
                .saturating_mul(2 * std::mem::size_of::<u128>())
                .saturating_add(*dictionary_budget),
        }
    }

    fn sort_current(&mut self) {
        match &mut self.storage {
            EncodedSortStorage::Fixed128 { records, .. } => {
                if !records.windows(2).any(|pair| pair[0] > pair[1]) {
                    records.dedup();
                } else {
                    parallel_sort_fixed128_in_place(records, self.sort_degree);
                }
            }
            EncodedSortStorage::Fixed {
                stride,
                bytes,
                order,
                ..
            } => {
                // Keep the compact arena immutable and sort its already-owned
                // ordinals in place. The full-record comparison preserves the
                // old stable-key/monotone-RowId total order.
                parallel_msd_sort_fixed_ordinals(bytes, *stride, order, self.sort_degree);
            }
            EncodedSortStorage::Variable { arena, records, .. } => {
                radix_sort_variable_descriptors(arena, records);
                records.dedup_by(|left, right| {
                    left.row_id == right.row_id
                        && descriptor_order_key(arena, left) == descriptor_order_key(arena, right)
                });
            }
            EncodedSortStorage::Interned { keys, records, .. } => {
                let mut key_ids = (0..keys.len() as u32).collect::<Vec<_>>();
                key_ids.sort_unstable_by(|left, right| {
                    keys[*left as usize]
                        .order_key
                        .cmp(&keys[*right as usize].order_key)
                });
                let mut rank_by_id = vec![0u32; keys.len()];
                let mut rank = 0u32;
                let mut previous: Option<u32> = None;
                for key_id in key_ids {
                    if previous.is_some_and(|prior| {
                        keys[prior as usize].order_key != keys[key_id as usize].order_key
                    }) {
                        rank = rank.saturating_add(1);
                    }
                    rank_by_id[key_id as usize] = rank;
                    previous = Some(key_id);
                }
                for record in records.iter_mut() {
                    let key_id = interned_record_key_id(*record);
                    *record = pack_interned_record(
                        rank_by_id[key_id as usize],
                        key_id,
                        interned_record_row_id(*record),
                    );
                }
                crate::util::par_sort::parallel_sort_unstable_dedup_by(
                    records,
                    self.sort_degree,
                    |left, right| (left >> 48).cmp(&(right >> 48)),
                );
            }
        }
    }

    fn flush_run(&mut self, data_dir: &Path) -> Result<(), QueryError> {
        if self.len() == 0 {
            return Ok(());
        }
        self.sort_current();
        if self.tmp.is_none() {
            self.tmp = Some(IndexSortTmpDir::create(data_dir)?);
        }
        let path = self
            .tmp
            .as_ref()
            .expect("encoded sorter owns temp dir")
            .path
            .join(format!("encoded-run-{}", self.runs.len()));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(io_err)?;
        let mut writer = std::io::BufWriter::with_capacity(1024 * 1024, file);
        match &self.storage {
            EncodedSortStorage::Fixed128 { records, .. } => {
                for encoded in records {
                    writer.write_all(&encoded.to_be_bytes()).map_err(io_err)?;
                }
            }
            EncodedSortStorage::Fixed {
                key_width,
                stride,
                bytes,
                order,
                ..
            } => {
                for ordinal in order {
                    let start = *ordinal as usize * *stride;
                    let record = &bytes[start..start + *stride];
                    write_encoded_run_record(
                        &mut writer,
                        &record[..*key_width],
                        None,
                        RowId {
                            page_id: u32::from_be_bytes(
                                record[*key_width..*key_width + 4].try_into().unwrap(),
                            ),
                            slot_id: u16::from_be_bytes(
                                record[*key_width + 4..].try_into().unwrap(),
                            ),
                        },
                        self.program.fixed_ops.as_ref().is_some_and(|ops| {
                            fixed_order_key_has_null(&record[..*key_width], ops)
                        }),
                    )?;
                }
            }
            EncodedSortStorage::Variable { arena, records, .. } => {
                for descriptor in records {
                    let payload = descriptor_materialized_key(arena, descriptor);
                    write_encoded_run_record(
                        &mut writer,
                        descriptor_order_key(arena, descriptor),
                        (!payload.is_empty()).then_some(payload),
                        descriptor.row_id,
                        descriptor.has_null,
                    )?;
                }
            }
            EncodedSortStorage::Interned { keys, records, .. } => {
                for record in records {
                    let key = &keys[interned_record_key_id(*record) as usize];
                    write_encoded_run_record(
                        &mut writer,
                        &key.order_key,
                        key.materialized_key.as_deref(),
                        interned_record_row_id(*record),
                        key.has_null,
                    )?;
                }
            }
        }
        writer.flush().map_err(io_err)?;
        if let Ok(meta) = fs::metadata(&path) {
            record_spill_bytes_written(meta.len());
        }
        self.runs.push(path);
        INDEX_BUILD_ENCODED_RUNS.fetch_add(1, Ordering::Relaxed);
        self.clear_current();
        Ok(())
    }

    fn clear_current(&mut self) {
        match &mut self.storage {
            EncodedSortStorage::Fixed128 { records, .. } => records.clear(),
            EncodedSortStorage::Fixed { bytes, order, .. } => {
                bytes.clear();
                order.clear();
            }
            EncodedSortStorage::Variable { arena, records, .. } => {
                arena.clear();
                records.clear();
            }
            EncodedSortStorage::Interned {
                keys,
                lookup,
                records,
                dictionary_bytes,
                ..
            } => {
                keys.clear();
                lookup.clear();
                records.clear();
                *dictionary_bytes = 0;
            }
        }
    }

    fn finish(mut self, data_dir: &Path) -> Result<EncodedSortedRecords, QueryError> {
        if self.runs.is_empty() {
            self.sort_current();
            return Ok(match self.storage {
                EncodedSortStorage::Fixed128 {
                    key_width,
                    stride,
                    records,
                } => EncodedSortedRecords::Fixed128 {
                    key_width,
                    stride,
                    records,
                },
                EncodedSortStorage::Fixed {
                    key_width,
                    stride,
                    bytes,
                    order,
                    ..
                } => EncodedSortedRecords::Fixed {
                    key_width,
                    stride,
                    bytes,
                    order,
                },
                EncodedSortStorage::Variable { arena, records, .. } => {
                    EncodedSortedRecords::Variable { arena, records }
                }
                EncodedSortStorage::Interned { keys, records, .. } => {
                    EncodedSortedRecords::Interned { keys, records }
                }
            });
        }
        self.flush_run(data_dir)?;
        let fixed128 = match &self.storage {
            EncodedSortStorage::Fixed128 {
                key_width, stride, ..
            } => Some((*key_width, *stride)),
            _ => None,
        };
        Ok(EncodedSortedRecords::Spilled {
            tmp: self.tmp.take().expect("encoded runs own temp dir"),
            runs: self.runs,
            fixed128,
        })
    }
}

fn encoded_arena_too_large(_: std::num::TryFromIntError) -> QueryError {
    error(
        crate::error::catalog::ER_OUT_OF_RESOURCES,
        "encoded index sort arena exceeds 4 GiB",
    )
}

fn descriptor_order_key<'a>(arena: &'a [u8], record: &EncodedVarDescriptor) -> &'a [u8] {
    &arena[record.order_start as usize..(record.order_start + record.order_len) as usize]
}

fn descriptor_materialized_key<'a>(arena: &'a [u8], record: &EncodedVarDescriptor) -> &'a [u8] {
    &arena[record.payload_start as usize..(record.payload_start + record.payload_len) as usize]
}

/// Fixed-size copies let the compiler encode each compact record without a
/// variable-length memcpy call. The caller has admitted the entire batch.
fn append_fixed128_records(records: &mut Vec<u128>, batch: &[u8], stride: usize) {
    fn append<const WIDTH: usize>(records: &mut Vec<u128>, batch: &[u8]) {
        records.extend(batch.chunks_exact(WIDTH).map(|record| {
            let mut encoded = [0u8; 16];
            encoded[..WIDTH].copy_from_slice(record);
            u128::from_be_bytes(encoded)
        }));
    }
    match stride {
        1 => append::<1>(records, batch),
        2 => append::<2>(records, batch),
        3 => append::<3>(records, batch),
        4 => append::<4>(records, batch),
        5 => append::<5>(records, batch),
        6 => append::<6>(records, batch),
        7 => append::<7>(records, batch),
        8 => append::<8>(records, batch),
        9 => append::<9>(records, batch),
        10 => append::<10>(records, batch),
        11 => append::<11>(records, batch),
        12 => append::<12>(records, batch),
        13 => append::<13>(records, batch),
        14 => append::<14>(records, batch),
        15 => append::<15>(records, batch),
        16 => append::<16>(records, batch),
        _ => unreachable!("compact records fit in u128"),
    }
}

const FIXED_MSD_COMPARISON_CUTOFF: usize = 128;

#[derive(Clone, Copy)]
struct FixedOrdinalSortTask {
    lo: usize,
    hi: usize,
    position: usize,
}

fn fixed_ordinal_cmp(bytes: &[u8], stride: usize, left: u32, right: u32) -> std::cmp::Ordering {
    let left = left as usize * stride;
    let right = right as usize * stride;
    bytes[left..left + stride].cmp(&bytes[right..right + stride])
}

/// Parallel out-of-place MSD partition sort for compact fixed records.
///
/// Records share every bit above their most significant varying bit, so the
/// next `PARTITION_BITS` varying bits (gathered past constant bits) form an
/// order-preserving bucket number. One parallel histogram pass and one
/// parallel scatter into a governed scratch Vec place every bucket, then
/// buckets sort independently. The sorted scratch (allocated at the source
/// capacity, so sorter admission is unchanged) replaces `records` and no
/// copy-back pass is needed. Returns false, leaving `records` untouched, when
/// the input is too small or the scratch cannot be admitted.
fn parallel_partition_sort_fixed128(records: &mut Vec<u128>, degree: usize) -> bool {
    const MIN_ROWS: usize = 1024 * 1024;
    const PARTITION_BITS: u32 = 11;
    let rows = records.len();
    if degree <= 1 || rows < MIN_ROWS {
        return false;
    }
    let Some(_scratch_charge) =
        crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            records
                .capacity()
                .saturating_mul(std::mem::size_of::<u128>()),
        )
    else {
        return false;
    };
    let workers = degree.min(rows / (MIN_ROWS / 8)).max(2);
    let chunk_rows = rows.div_ceil(workers);
    let first = records[0];
    let varying = std::thread::scope(|scope| {
        records
            .chunks(chunk_rows)
            .map(|chunk| {
                scope.spawn(move || {
                    chunk
                        .iter()
                        .fold(0u128, |bits, record| bits | (record ^ first))
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .fold(0u128, |bits, handle| {
                bits | handle.join().expect("fixed partition mask worker panicked")
            })
    });
    if varying == 0 {
        records.truncate(1);
        return true;
    }
    // Most significant varying bit positions, highest first.
    let mut positions = Vec::with_capacity(PARTITION_BITS as usize);
    let mut remaining = varying;
    while remaining != 0 && positions.len() < PARTITION_BITS as usize {
        let position = 127 - remaining.leading_zeros();
        positions.push(position);
        remaining &= !(1u128 << position);
    }
    let positions = positions.as_slice();
    let bucket_of = move |record: u128| {
        positions.iter().fold(0usize, |bucket, position| {
            (bucket << 1) | ((record >> position) & 1) as usize
        })
    };
    let buckets = 1usize << positions.len();
    let histograms = std::thread::scope(|scope| {
        records
            .chunks(chunk_rows)
            .map(|chunk| {
                scope.spawn(move || {
                    let mut counts = vec![0usize; buckets];
                    for record in chunk {
                        counts[bucket_of(*record)] += 1;
                    }
                    counts
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .expect("fixed partition histogram worker panicked")
            })
            .collect::<Vec<_>>()
    });
    // Bucket-major, then chunk-major, destination offsets.
    let mut starts = vec![vec![0usize; buckets]; histograms.len()];
    let mut bounds = Vec::with_capacity(buckets + 1);
    let mut offset = 0usize;
    for bucket in 0..buckets {
        bounds.push(offset);
        for (chunk, counts) in histograms.iter().enumerate() {
            starts[chunk][bucket] = offset;
            offset += counts[bucket];
        }
    }
    bounds.push(offset);
    debug_assert_eq!(offset, rows);
    let mut scratch = Vec::<u128>::with_capacity(records.capacity());
    struct ScatterTarget(*mut u128);
    // SAFETY: every worker writes only the disjoint destination slots
    // reserved for its chunk by the prefix sums above.
    unsafe impl Sync for ScatterTarget {}
    let target = ScatterTarget(scratch.as_mut_ptr());
    std::thread::scope(|scope| {
        for (chunk, mut cursor) in records.chunks(chunk_rows).zip(starts) {
            let target = &target;
            scope.spawn(move || {
                for record in chunk {
                    let slot = &mut cursor[bucket_of(*record)];
                    // SAFETY: `slot` stays inside this chunk's reserved range
                    // of `0..rows`, within the scratch capacity.
                    unsafe { target.0.add(*slot).write(*record) };
                    *slot += 1;
                }
            });
        }
    });
    // SAFETY: the scatter initialized each of the `rows` slots exactly once.
    unsafe { scratch.set_len(rows) };
    let mut slices = Vec::new();
    let mut rest = scratch.as_mut_slice();
    for range in bounds.windows(2) {
        let (bucket, tail) = rest.split_at_mut(range[1] - range[0]);
        if bucket.len() > 1 {
            slices.push(bucket);
        }
        rest = tail;
    }
    // Pop the largest buckets first so a skewed bucket cannot start last.
    slices.sort_unstable_by_key(|bucket| bucket.len());
    let sorters = degree.min(slices.len()).max(1);
    let queue = Mutex::new(slices);
    std::thread::scope(|scope| {
        for _ in 0..sorters {
            scope.spawn(|| loop {
                let Some(bucket) = queue.lock().unwrap_or_else(|e| e.into_inner()).pop() else {
                    return;
                };
                if !try_sort_compact_fixed128(bucket) {
                    bucket.sort_unstable();
                }
            });
        }
    });
    *records = scratch;
    records.dedup();
    true
}

/// Split a fixed-width run into ordered rank ranges without a second arena.
/// `select_nth_unstable` guarantees every left value is <= every right value;
/// each disjoint range can then be sorted on its own worker and deduplicated
/// once across all boundaries.
fn parallel_sort_fixed128_in_place(records: &mut Vec<u128>, degree: usize) {
    const MIN_PARALLEL_ROWS: usize = 256 * 1024;
    if degree > 1 && parallel_partition_sort_fixed128(records, degree) {
        return;
    }
    if degree <= 1 || records.len() < MIN_PARALLEL_ROWS {
        if !try_sort_compact_fixed128(records) {
            records.sort_unstable();
        }
        records.dedup();
        return;
    }

    fn partition<'a>(data: &'a mut [u128], parts: usize, output: &mut Vec<&'a mut [u128]>) {
        if parts <= 1 {
            output.push(data);
            return;
        }
        let left_parts = parts / 2;
        let midpoint = data.len() * left_parts / parts;
        data.select_nth_unstable(midpoint);
        let (left, right) = data.split_at_mut(midpoint);
        partition(left, left_parts, output);
        partition(right, parts - left_parts, output);
    }

    let workers = degree.min(records.len() / (MIN_PARALLEL_ROWS / 2)).max(2);
    std::thread::scope(|scope| {
        let mut partitions = Vec::with_capacity(workers);
        partition(records, workers, &mut partitions);
        for partition in partitions {
            scope.spawn(move || partition.sort_unstable());
        }
    });
    records.dedup();
}

/// Discard only bits that are identical in every record. Extracting the
/// remaining bits preserves the unsigned total order, including RowId ties.
/// Packing and restoring reuse the input allocation, so run admission and
/// spill boundaries are unchanged.
fn try_sort_compact_fixed128(records: &mut [u128]) -> bool {
    #[cfg(target_arch = "x86_64")]
    if records.len() >= 64 * 1024 && std::is_x86_feature_detected!("bmi2") {
        // The comparison sorter handles long monotone runs especially well;
        // avoid paying two conversion passes for those inputs.
        let inversions = records
            .windows(2)
            .take(1024)
            .filter(|pair| pair[0] > pair[1])
            .count();
        if !(64..=960).contains(&inversions) {
            return false;
        }
        // SAFETY: BMI2 was checked at runtime. The helper stays inside the
        // initialized allocation and restores every u128 before returning.
        return unsafe { sort_compact_fixed128_bmi2(records) };
    }
    let _ = records;
    false
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "bmi2")]
unsafe fn sort_compact_fixed128_bmi2(records: &mut [u128]) -> bool {
    use std::arch::x86_64::{_pdep_u64, _pext_u64};
    let Some(&first) = records.first() else {
        return true;
    };
    let mut varying = 0u128;
    for &record in records.iter() {
        varying |= record ^ first;
        if varying.count_ones() > 64 {
            return false;
        }
    }
    let low_mask = varying as u64;
    let high_mask = (varying >> 64) as u64;
    let low_bits = low_mask.count_ones();
    let fixed = first & !varying;
    let length = records.len();
    let pointer = records.as_mut_ptr();
    // SAFETY: forward packing writes at 8*i only after reading at 16*i.
    // Therefore it never overwrites a future source record. u128 alignment
    // is sufficient for u64, and all bit patterns of both types are valid.
    unsafe {
        for index in 0..length {
            let record = *pointer.add(index);
            let low = _pext_u64(record as u64, low_mask);
            let compact = if low_bits == 64 {
                low
            } else {
                low | (_pext_u64((record >> 64) as u64, high_mask) << low_bits)
            };
            *pointer.cast::<u64>().add(index) = compact;
        }
        std::slice::from_raw_parts_mut(pointer.cast::<u64>(), length).sort_unstable();
        // Reverse expansion cannot overwrite a compact value not yet read.
        for index in (0..length).rev() {
            let compact = *pointer.cast::<u64>().add(index);
            let high = if low_bits == 64 {
                0
            } else {
                _pdep_u64(compact >> low_bits, high_mask)
            };
            *pointer.add(index) =
                fixed | u128::from(_pdep_u64(compact, low_mask)) | (u128::from(high) << 64);
        }
    }
    true
}

/// In-place MSD radix over compact ordinals into an immutable fixed-record
/// arena. The coordinator first creates disjoint radix buckets; workers then
/// claim those ranges through an atomic queue, so at most `degree` threads are
/// active and no arena-sized output scratch is needed.
fn parallel_msd_sort_fixed_ordinals(bytes: &[u8], stride: usize, order: &mut [u32], degree: usize) {
    if order.len() < 2 {
        return;
    }
    let target_tasks = degree.max(1).saturating_mul(8);
    let mut tasks = vec![FixedOrdinalSortTask {
        lo: 0,
        hi: order.len(),
        position: 0,
    }];
    while tasks.len() < target_tasks {
        let Some((slot, task)) = tasks
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, task)| task.hi - task.lo > FIXED_MSD_COMPARISON_CUTOFF)
            .max_by_key(|(_, task)| task.hi - task.lo)
        else {
            break;
        };
        tasks.swap_remove(slot);
        partition_fixed_ordinal_task(bytes, stride, order, task, &mut tasks);
    }
    if tasks.is_empty() {
        return;
    }

    let workers = degree.max(1).min(tasks.len());
    let next = AtomicUsize::new(0);
    let tasks = &tasks;
    let order_addr = order.as_mut_ptr() as usize;
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let next = &next;
            scope.spawn(move || loop {
                let slot = next.fetch_add(1, Ordering::Relaxed);
                let Some(task) = tasks.get(slot).copied() else {
                    break;
                };
                // SAFETY: radix partitioning produced pairwise-disjoint task
                // ranges, and the atomic queue hands each task to exactly one
                // worker. `order` remains alive and fixed-capacity for the
                // whole scoped join; `bytes` is immutable.
                let range = unsafe {
                    std::slice::from_raw_parts_mut(
                        (order_addr as *mut u32).add(task.lo),
                        task.hi - task.lo,
                    )
                };
                msd_sort_fixed_ordinal_range(bytes, stride, range, task.position);
            });
        }
    });
}

fn partition_fixed_ordinal_task(
    bytes: &[u8],
    stride: usize,
    order: &mut [u32],
    mut task: FixedOrdinalSortTask,
    output: &mut Vec<FixedOrdinalSortTask>,
) {
    while task.position < stride {
        let slice = &mut order[task.lo..task.hi];
        let counts = partition_fixed_ordinal_slice(bytes, stride, slice, task.position);
        let non_empty = counts.iter().filter(|count| **count != 0).count();
        if non_empty <= 1 {
            task.position += 1;
            continue;
        }
        let mut cursor = task.lo;
        for count in counts {
            if count > 1 {
                output.push(FixedOrdinalSortTask {
                    lo: cursor,
                    hi: cursor + count,
                    position: task.position + 1,
                });
            }
            cursor += count;
        }
        return;
    }
}

fn partition_fixed_ordinal_slice(
    bytes: &[u8],
    stride: usize,
    order: &mut [u32],
    position: usize,
) -> [usize; 256] {
    let bucket = |ordinal: u32| usize::from(bytes[ordinal as usize * stride + position]);
    let mut counts = [0usize; 256];
    for ordinal in order.iter().copied() {
        counts[bucket(ordinal)] += 1;
    }
    let mut starts = [0usize; 256];
    let mut cursor = 0usize;
    for (slot, count) in counts.iter().copied().enumerate() {
        starts[slot] = cursor;
        cursor += count;
    }
    let mut next = starts;
    for expected in 0..256 {
        let end = starts[expected] + counts[expected];
        while next[expected] < end {
            let actual = bucket(order[next[expected]]);
            if actual == expected {
                next[expected] += 1;
            } else {
                order.swap(next[expected], next[actual]);
                next[actual] += 1;
            }
        }
    }
    counts
}

fn msd_sort_fixed_ordinal_range(bytes: &[u8], stride: usize, order: &mut [u32], position: usize) {
    let mut pending = vec![(0usize, order.len(), position)];
    while let Some((lo, hi, position)) = pending.pop() {
        if hi - lo <= FIXED_MSD_COMPARISON_CUTOFF {
            order[lo..hi]
                .sort_unstable_by(|left, right| fixed_ordinal_cmp(bytes, stride, *left, *right));
            continue;
        }
        if position == stride {
            continue;
        }
        let counts = partition_fixed_ordinal_slice(bytes, stride, &mut order[lo..hi], position);
        let mut cursor = lo;
        let mut non_empty = 0usize;
        for count in counts {
            non_empty += usize::from(count != 0);
            if count > 1 {
                pending.push((cursor, cursor + count, position + 1));
            }
            cursor += count;
        }
        if non_empty <= 1 {
            // The sole pending child advances over this constant byte.
            continue;
        }
    }
}

/// Lexicographic MSD radix over arbitrary encoded order-key bytes. Bucket 0
/// is the end-of-key sentinel and byte values occupy buckets 1..=256, making
/// a shorter equal-prefix key sort before its extension exactly like slice
/// comparison. Small partitions use the comparator oracle; equal complete
/// keys only sort their RowIds.
fn radix_sort_variable_descriptors(arena: &[u8], records: &mut [EncodedVarDescriptor]) {
    const COMPARISON_CUTOFF: usize = 96;
    if records.len() < 2 {
        return;
    }
    let compare = |left: &EncodedVarDescriptor, right: &EncodedVarDescriptor| {
        descriptor_order_key(arena, left)
            .cmp(descriptor_order_key(arena, right))
            .then_with(|| left.row_id.cmp(&right.row_id))
    };
    if records
        .windows(2)
        .all(|pair| !compare(&pair[0], &pair[1]).is_gt())
    {
        return;
    }
    let mut pending = vec![(0usize, records.len(), 0usize)];
    while let Some((lo, hi, position)) = pending.pop() {
        if hi - lo <= COMPARISON_CUTOFF {
            records[lo..hi].sort_unstable_by(&compare);
            continue;
        }
        let bucket = |record: &EncodedVarDescriptor| {
            descriptor_order_key(arena, record)
                .get(position)
                .map_or(0usize, |byte| usize::from(*byte) + 1)
        };
        let mut counts = [0usize; 257];
        for record in &records[lo..hi] {
            counts[bucket(record)] += 1;
        }
        if counts.iter().filter(|count| **count != 0).count() == 1 {
            if counts[0] != 0 {
                records[lo..hi].sort_unstable_by_key(|record| record.row_id);
            } else {
                pending.push((lo, hi, position + 1));
            }
            continue;
        }
        let mut starts = [0usize; 257];
        let mut cursor = lo;
        for (slot, count) in counts.iter().copied().enumerate() {
            starts[slot] = cursor;
            cursor += count;
        }
        let mut next = starts;
        for expected in 0..257 {
            let end = starts[expected] + counts[expected];
            while next[expected] < end {
                let actual = bucket(&records[next[expected]]);
                if actual == expected {
                    next[expected] += 1;
                } else {
                    records.swap(next[expected], next[actual]);
                    next[actual] += 1;
                }
            }
        }
        if counts[0] > 1 {
            records[starts[0]..starts[0] + counts[0]].sort_unstable_by_key(|record| record.row_id);
        }
        for slot in (1..257).rev() {
            if counts[slot] > 1 {
                pending.push((starts[slot], starts[slot] + counts[slot], position + 1));
            }
        }
    }
}

fn fixed_order_key_has_null(bytes: &[u8], ops: &[FixedKeyOp]) -> bool {
    let mut offset = 0usize;
    for op in ops {
        if bytes[offset] == 0 {
            return true;
        }
        offset += op.width();
    }
    false
}

fn write_encoded_run_record<W: Write>(
    writer: &mut W,
    order_key: &[u8],
    materialized_key: Option<&[u8]>,
    row_id: RowId,
    has_null: bool,
) -> Result<(), QueryError> {
    writer
        .write_all(&(order_key.len() as u32).to_le_bytes())
        .and_then(|_| {
            writer.write_all(&(materialized_key.map_or(0, <[u8]>::len) as u32).to_le_bytes())
        })
        .and_then(|_| writer.write_all(&row_id.page_id.to_le_bytes()))
        .and_then(|_| writer.write_all(&row_id.slot_id.to_le_bytes()))
        .and_then(|_| writer.write_all(&[u8::from(has_null)]))
        .and_then(|_| writer.write_all(order_key))
        .and_then(|_| writer.write_all(materialized_key.unwrap_or_default()))
        .map_err(io_err)
}

fn encoded_btree_build_eligible(index: &IndexSchema) -> bool {
    index.index_type.eq_ignore_ascii_case("BTREE")
        && !index.is_fulltext()
        && !index.is_spatial()
        && !index.is_vector()
}

const MIN_ENCODED_INDEX_SORTER_BYTES: usize = 4 * 1024 * 1024;

struct OfflineIndexScratchAdmission {
    gate: Mutex<()>,
    available: Condvar,
}

fn offline_index_scratch_admission() -> &'static OfflineIndexScratchAdmission {
    static ADMISSION: OnceLock<OfflineIndexScratchAdmission> = OnceLock::new();
    ADMISSION.get_or_init(|| OfflineIndexScratchAdmission {
        gate: Mutex::new(()),
        available: Condvar::new(),
    })
}

fn lock_offline_index_scratch_admission() -> Result<MutexGuard<'static, ()>, QueryError> {
    offline_index_scratch_admission().gate.lock().map_err(|_| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "offline index scratch admission lock poisoned",
        )
    })
}

fn wait_for_offline_index_scratch(guard: MutexGuard<'static, ()>) -> Result<(), QueryError> {
    let (guard, _) = offline_index_scratch_admission()
        .available
        .wait_timeout(guard, Duration::from_secs(1))
        .map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "offline index scratch admission lock poisoned while waiting",
            )
        })?;
    drop(guard);
    Ok(())
}

/// Global operational room left outside sorter envelopes for parser/protocol
/// buffers, direct heap/index batches and WAL encoding.  Five percent scales
/// down on small targets; 8..64 MiB bounds keep both the direct-build minimum
/// and high-connection bulk-prepare workloads usable.
fn index_build_ancillary_headroom_for_limit(operational_limit: usize) -> usize {
    (operational_limit / 20)
        .clamp(8 * 1024 * 1024, 64 * 1024 * 1024)
        .min(operational_limit / 4)
}

fn clamp_index_build_scratch_to_headroom(requested: usize, minimum: usize) -> usize {
    let operational_limit = crate::engine::executor::mem_budget::operational_memory_limit_bytes();
    let controlled = crate::engine::executor::mem_budget::controlled_memory_snapshot().0;
    let available = operational_limit
        .saturating_sub(controlled)
        .saturating_sub(index_build_ancillary_headroom_for_limit(operational_limit));
    requested.min(available).max(minimum)
}

/// Capacity state shared by every offline index build in one database.
///
/// A stack of independent `old_capacity` values is not sufficient here:
/// builds on different tables finish in arbitrary order.  If an older build
/// restored its large capacity while a newer build still owned a smaller
/// floor, foreground INSERTs could refill the pool and consume the newer
/// build's governed sorter headroom.  Keep a multiset of active floors and
/// restore the configured capacity only after the final lease leaves.
#[derive(Debug, Default)]
struct OfflineIndexBuildBufferLeaseState {
    restore_capacity_pages: Option<u64>,
    active_limits: BTreeMap<u64, u64>,
    next_lease_id: u64,
}

impl OfflineIndexBuildBufferLeaseState {
    fn add(&mut self, current_capacity_pages: u64, limit_pages: u64) -> (u64, u64) {
        if self.active_limits.is_empty() {
            self.restore_capacity_pages = Some(current_capacity_pages.max(1));
        }
        self.next_lease_id = self.next_lease_id.wrapping_add(1).max(1);
        let lease_id = self.next_lease_id;
        self.active_limits.insert(lease_id, limit_pages.max(1));
        (lease_id, self.effective_capacity_pages())
    }

    fn remove(&mut self, lease_id: u64) -> Option<u64> {
        self.active_limits.remove(&lease_id)?;
        if self.active_limits.is_empty() {
            self.restore_capacity_pages.take()
        } else {
            Some(self.effective_capacity_pages())
        }
    }

    fn effective_capacity_pages(&self) -> u64 {
        self.active_limits
            .values()
            .copied()
            .min()
            .or(self.restore_capacity_pages)
            .unwrap_or(1)
            .max(1)
    }

    fn effective_for_configured_capacity(&self, configured_pages: u64) -> u64 {
        self.active_limits
            .values()
            .copied()
            .min()
            .map_or(configured_pages.max(1), |floor| {
                floor.min(configured_pages.max(1))
            })
    }
}

/// Apply a runtime buffer-pool resize without invalidating active index-build
/// headroom. The configured value becomes the eventual restore target while
/// the smallest temporary floor remains effective until its owner exits.
fn set_configured_buffer_pool_capacity(
    db: &Database,
    configured_pages: u64,
    timeout: Duration,
) -> Result<(), QueryError> {
    let mut state = db.offline_index_buffer_leases.lock().map_err(|_| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "offline index build buffer lease lock poisoned",
        )
    })?;
    let effective_pages = state.effective_for_configured_capacity(configured_pages);
    db.buffer_pool.set_capacity(effective_pages, timeout)?;
    if !state.active_limits.is_empty() {
        state.restore_capacity_pages = Some(configured_pages.max(1));
    }
    Ok(())
}

/// Temporary buffer-pool ceiling used while a large offline index owns its
/// scratch reservation. Evicted raw pages are normally retained in the
/// recycled-page pool, which is desirable for steady-state OLTP but does not
/// return anonymous memory to a cgroup. Lowering the shared capacity makes
/// those recycled leases drop; restoring the configured ceiling is
/// allocation-free.
struct OfflineIndexBuildBufferLease<'a> {
    db: &'a Database,
    lease_id: u64,
}

impl Drop for OfflineIndexBuildBufferLease<'_> {
    fn drop(&mut self) {
        let Ok(mut state) = self.db.offline_index_buffer_leases.lock() else {
            log_message("offline index build: buffer lease state poisoned while releasing");
            return;
        };
        let Some(capacity_pages) = state.remove(self.lease_id) else {
            return;
        };
        if let Err(err) = self
            .db
            .buffer_pool
            .set_capacity(capacity_pages, Duration::from_secs(30))
        {
            log_message(&format!(
                "offline index build: failed to restore shared buffer-pool capacity: {err:?}"
            ));
        }
        offline_index_scratch_admission().available.notify_all();
    }
}

#[inline]
fn governed_index_build_reclaim_bytes(controlled_bytes: usize, requested_scratch: usize) -> usize {
    let operational_limit = crate::engine::executor::mem_budget::operational_memory_limit_bytes();
    governed_index_build_reclaim_bytes_for_values(
        controlled_bytes,
        requested_scratch
            .saturating_add(index_build_ancillary_headroom_for_limit(operational_limit)),
        operational_limit,
    )
}

#[inline]
fn governed_index_build_reclaim_bytes_for_values(
    controlled_bytes: usize,
    requested_scratch: usize,
    operational_limit: usize,
) -> usize {
    controlled_bytes
        .saturating_add(requested_scratch)
        .saturating_sub(operational_limit)
}

fn reclaim_index_build_headroom<'a>(
    db: &'a Database,
    requested_scratch: usize,
) -> Result<OfflineIndexBuildBufferLease<'a>, QueryError> {
    // Serialise only the short reclaim/admission transition. Index scans and
    // sorts remain concurrent, but a finishing peer cannot restore capacity
    // between this build's flush and lease registration.
    let mut state = db.offline_index_buffer_leases.lock().map_err(|_| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "offline index build buffer lease lock poisoned",
        )
    })?;
    let controlled = crate::engine::executor::mem_budget::controlled_memory_snapshot().0;
    let resident_reclaim_bytes =
        crate::engine::memory_reclaimer::bulk_allocation_pressure_reclaim_bytes(
            requested_scratch,
            85,
        );
    // RSS can still be below its 85% pressure watermark while the controlled
    // governor is already at its stricter 80% operational ceiling.  Reclaim
    // whichever deficit is larger so the forthcoming spillable sorter can be
    // admitted instead of failing at its minimum run size.
    let reclaim_bytes = resident_reclaim_bytes.max(governed_index_build_reclaim_bytes(
        controlled,
        requested_scratch,
    ));

    if reclaim_bytes != 0 {
        db.flush_data_cache()?;
    }
    let (_, _, categories, _) = crate::engine::executor::mem_budget::controlled_memory_snapshot();
    let raw_bytes = categories[0];
    let old_capacity_pages = db.buffer_pool.capacity_pages();
    let min_buffer_bytes = (16 * 1024 * 1024)
        .min(crate::engine::executor::mem_budget::memory_target_bytes() / 4)
        .max(PAGE_SIZE);
    let desired_pages = if reclaim_bytes == 0 {
        old_capacity_pages
    } else {
        let desired_bytes = raw_bytes
            .saturating_sub(reclaim_bytes)
            .max(min_buffer_bytes);
        (desired_bytes / PAGE_SIZE).max(1) as u64
    };
    // Register even when this request does not lower the current floor.  It
    // may have observed a cap owned by another active build and must keep that
    // cap alive if the peer finishes first.
    let (lease_id, effective_pages) = state.add(old_capacity_pages, desired_pages);
    if let Err(err) = db
        .buffer_pool
        .set_capacity(effective_pages, Duration::from_secs(30))
    {
        let _ = state.remove(lease_id);
        return Err(err);
    }
    drop(state);
    if reclaim_bytes != 0 {
        let _ = crate::engine::memory_reclaimer::release_allocator_cached_memory();
    }
    Ok(OfflineIndexBuildBufferLease { db, lease_id })
}

fn reserve_offline_index_build_scratch<'a>(
    db: &'a Database,
    requested: usize,
    minimum: usize,
) -> Result<
    (
        OfflineIndexBuildBufferLease<'a>,
        crate::engine::executor::mem_budget::ControlledMemoryCharge,
        usize,
    ),
    QueryError,
> {
    use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};

    loop {
        let admission_guard = lock_offline_index_scratch_admission()?;
        let (controlled, _, categories, _) =
            crate::engine::executor::mem_budget::controlled_memory_snapshot();
        let operational_limit =
            crate::engine::executor::mem_budget::operational_memory_limit_bytes();
        let ancillary_headroom = index_build_ancillary_headroom_for_limit(operational_limit);
        let min_buffer_bytes = (16 * 1024 * 1024)
            .min(crate::engine::executor::mem_budget::memory_target_bytes() / 4)
            .max(PAGE_SIZE);
        let peers_active = db
            .offline_index_buffer_leases
            .lock()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "offline index build buffer lease lock poisoned",
                )
            })?
            .active_limits
            .len();
        if peers_active != 0
            && categories[0] <= min_buffer_bytes
            && controlled
                .saturating_add(minimum)
                .saturating_add(ancillary_headroom)
                > operational_limit
        {
            wait_for_offline_index_scratch(admission_guard)?;
            continue;
        }
        let buffer_lease = reclaim_index_build_headroom(db, requested)?;
        let mut granted = clamp_index_build_scratch_to_headroom(requested, minimum);
        let charge = loop {
            if let Some(charge) = ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::StorageScratch,
                granted,
            ) {
                break Some(charge);
            }
            if granted <= minimum {
                break None;
            }
            granted = (granted / 2).max(minimum);
        };
        if let Some(charge) = charge {
            drop(admission_guard);
            return Ok((buffer_lease, charge, granted));
        }

        // Spillable offline builds queue behind peers when even their minimum
        // run cannot be admitted. Returning 1037 here merely because earlier
        // builders obtained their envelopes would make concurrency determine
        // DDL correctness. Release our capacity dependency and retry when a
        // peer returns its sorter charge.
        drop(buffer_lease);
        let peers_active = db
            .offline_index_buffer_leases
            .lock()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "offline index build buffer lease lock poisoned",
                )
            })?
            .active_limits
            .len();
        if peers_active == 0 {
            drop(admission_guard);
            return Err(ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::StorageScratch,
                minimum,
            )
            .expect_err("minimum offline index scratch optional reservation just failed"));
        }
        wait_for_offline_index_scratch(admission_guard)?;
    }
}

fn poisoned_sorter_error() -> QueryError {
    error(
        crate::error::catalog::ER_UNKNOWN_ERROR,
        "encoded index sorter mutex poisoned",
    )
}

/// Move one scan worker's per-sorter chunks into the shared sorters. Every
/// sorter has its own lock. A worker starts at its own offset and skips busy
/// sorters, blocking only when all pending sorters are held, so concurrent
/// flushes (including first-touch faults of sorter capacity) spread across
/// locks instead of convoying behind one table-wide lock.
fn flush_chunks_to_sorters<C>(
    sorters: &[Mutex<EncodedIndexSorter>],
    chunks: &mut [C],
    start: usize,
    is_empty: impl Fn(&C) -> bool,
    mut push: impl FnMut(&mut EncodedIndexSorter, &mut C) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    debug_assert_eq!(sorters.len(), chunks.len());
    let count = sorters.len();
    let mut remaining = chunks.iter().filter(|chunk| !is_empty(chunk)).count();
    let mut wait = false;
    while remaining != 0 {
        let mut flushed = false;
        for step in 0..count {
            let index = (start + step) % count;
            if is_empty(&chunks[index]) {
                continue;
            }
            let mut sorter = if wait {
                sorters[index].lock().map_err(|_| poisoned_sorter_error())?
            } else {
                match sorters[index].try_lock() {
                    Ok(sorter) => sorter,
                    Err(std::sync::TryLockError::WouldBlock) => continue,
                    Err(std::sync::TryLockError::Poisoned(_)) => {
                        return Err(poisoned_sorter_error())
                    }
                }
            };
            push(&mut sorter, &mut chunks[index])?;
            remaining -= 1;
            flushed = true;
            wait = false;
        }
        wait = !flushed;
    }
    Ok(())
}

fn into_inner_sorters(
    sorters: Vec<Mutex<EncodedIndexSorter>>,
) -> Result<Vec<EncodedIndexSorter>, QueryError> {
    sorters
        .into_iter()
        .map(|sorter| sorter.into_inner().map_err(|_| poisoned_sorter_error()))
        .collect()
}

/// Budget weights for `proportional_encoded_index_budgets`: each sorter's
/// estimated resident capacity above the per-sorter minimum, so a full grant
/// gives every sorter exactly its estimate. Without estimates, split evenly.
fn encoded_sorter_budget_weights(estimates: Option<&[usize]>, sorters: usize) -> Vec<usize> {
    match estimates {
        Some(estimates) => estimates
            .iter()
            .map(|bytes| bytes.saturating_sub(MIN_ENCODED_INDEX_SORTER_BYTES).max(1))
            .collect(),
        None => vec![1; sorters],
    }
}

fn proportional_encoded_index_budgets(weights: &[usize], aggregate_budget: usize) -> Vec<usize> {
    if weights.is_empty() {
        return Vec::new();
    }
    let minimum_total = weights.len().saturating_mul(MIN_ENCODED_INDEX_SORTER_BYTES);
    let distributable = aggregate_budget.saturating_sub(minimum_total);
    let total_weight = weights
        .iter()
        .fold(0usize, |sum, weight| sum.saturating_add(*weight))
        .max(1);
    let mut assigned = 0usize;
    let mut budgets = Vec::with_capacity(weights.len());
    for (position, weight) in weights.iter().enumerate() {
        let extra = if position + 1 == weights.len() {
            distributable.saturating_sub(assigned)
        } else {
            distributable.saturating_mul(*weight) / total_weight
        };
        assigned = assigned.saturating_add(extra);
        budgets.push(MIN_ENCODED_INDEX_SORTER_BYTES.saturating_add(extra));
    }
    budgets
}

/// Exact governed capacity required by the encoded sorter's resident source
/// and sorting scratch for `rows` fixed-width records.
///
/// Fixed128 records are rank-partitioned and sorted in place. Wide fixed
/// records use an immutable arena plus in-place MSD-sorted u32 ordinals.
fn encoded_fixed_capacity_bytes_for_rows(
    program: &CompiledIndexKeyProgram,
    rows: usize,
) -> Option<usize> {
    let width = program.fixed_record_width()?;
    if width <= std::mem::size_of::<u128>() {
        rows.checked_mul(std::mem::size_of::<u128>())
    } else {
        rows.checked_mul(width.checked_add(std::mem::size_of::<u32>())?)
    }
}

impl Database {
    /// Unified offline builder for every B-tree key shape. The caller owns the
    /// table X lock and publishes catalog state only after all files finish.
    fn rebuild_indexes_offline_encoded_batch(
        &self,
        table: &TableSchema,
        index_names: &[String],
        options: IndexBuildOptions,
    ) -> Result<(), QueryError> {
        self.rebuild_indexes_encoded_batch(table, index_names, options, false, &[])
            .map(|_| ())
    }

    fn rebuild_indexes_encoded_batch(
        &self,
        table: &TableSchema,
        index_names: &[String],
        options: IndexBuildOptions,
        online: bool,
        validation_indexes: &[String],
    ) -> Result<HashMap<String, IndexBuildValidation>, QueryError> {
        let generation_lsn = if physical_wal_apply_active() {
            btree_logical_wal::current().unwrap_or(0)
        } else {
            self.log.lock().last_lsn()
        };
        let mut indexes = Vec::with_capacity(index_names.len());
        let mut programs = Vec::with_capacity(index_names.len());
        for name in index_names {
            let index = table
                .indexes
                .iter()
                .find(|index| index.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("encoded ADD INDEX missing index '{name}'"),
                    )
                })?;
            if !encoded_btree_build_eligible(index) {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("encoded B-tree builder cannot build '{}'", index.name),
                ));
            }
            indexes.push(index.clone());
            programs.push(CompiledIndexKeyProgram::compile(table, index)?);
        }
        if indexes.is_empty() {
            return Ok(HashMap::new());
        }

        let needs_full_row = programs
            .iter()
            .any(|program| program.direct_parts.is_none());
        let needed_columns = if needs_full_row {
            (0..table.columns.len()).collect::<Vec<_>>()
        } else {
            let mut columns = BTreeSet::new();
            for program in &programs {
                columns.extend(
                    program
                        .source_columns()
                        .expect("direct program exposes source columns"),
                );
            }
            columns.into_iter().collect::<Vec<_>>()
        };
        let offsets = needed_columns
            .iter()
            .enumerate()
            .map(|(offset, column)| (*column, offset))
            .collect::<HashMap<_, _>>();
        let programs = programs
            .into_iter()
            .map(|program| {
                if program.direct_parts.is_none() {
                    Arc::new(program)
                } else {
                    Arc::new(
                        program
                            .with_projection(&offsets)
                            .expect("projected columns contain compiled key inputs"),
                    )
                }
            })
            .collect::<Vec<_>>();

        for index in &indexes {
            if !online {
                self.remove_index_cache_entry(table.id, &index.name)?;
            }
            let path = self.index_path_for_table_schema(table, table.id, &index.name);
            self.buffer_pool
                .discard_file(&Arc::new(BufferPool::index_file_key(path)))?;
        }

        let fixed_parallel = programs.iter().all(|program| {
            program.direct_parts.is_some()
                && program.fixed_ops.as_ref().is_some_and(|ops| {
                    ops.iter()
                        .all(|op| !matches!(op, FixedKeyOp::DecimalWide { .. }))
                })
        });
        let projected_parallel = programs
            .iter()
            .all(|program| program.direct_parts.is_some());
        let page_count = self.with_heap_read(table.id, |heap| Ok(heap.page_count_relaxed()))?;
        const ENCODED_SCAN_CHUNK_ROWS: usize = 4096;
        const ENCODED_SCAN_BATCH_BYTES: usize = 4 * 1024 * 1024;
        let scan_degree = if projected_parallel {
            options
                .effective_degree
                .max(1)
                .min(page_count.max(1) as usize)
        } else {
            1
        };
        let task_pages = encoded_scan_task_pages(page_count, scan_degree);
        let scan_degree = scan_degree.min(page_count.div_ceil(task_pages).max(1) as usize);
        let chunk_bytes = programs
            .iter()
            .filter_map(|program| program.fixed_record_width())
            .sum::<usize>()
            .saturating_mul(ENCODED_SCAN_CHUNK_ROWS)
            .saturating_mul(scan_degree);
        let parallel_scratch = scan_degree
            .saturating_mul(4 * 1024 * 1024)
            .saturating_add(chunk_bytes)
            .saturating_add(
                (!fixed_parallel as usize)
                    .saturating_mul(scan_degree.saturating_mul(8 * 1024 * 1024)),
            );
        let row_count = self.index_build_row_count_hint(table);
        // Admit the whole scan phase before assigning spillable run capacity.
        // Worker staging must never grow into the direct-read progress reserve.
        let sorter_minimum = indexes.len().saturating_mul(4 * 1024 * 1024);
        let prepaid_scan = fixed_parallel && scan_degree > 1;
        let scan_minimum = if prepaid_scan {
            scan_degree * 1024 * 1024
        } else {
            0
        };
        let minimum = sorter_minimum + parallel_scratch + scan_minimum;
        let program_estimates = programs
            .iter()
            .map(|program| {
                let rows = row_count
                    .saturating_add(row_count / 100)
                    .saturating_add(4096);
                let bytes = encoded_fixed_capacity_bytes_for_rows(program, rows).or_else(|| {
                    // A promoted variable dictionary keeps one u128 source record
                    // plus an equally-sized parallel-sort scratch record. Reserve
                    // an additional one-eighth envelope for dictionary keys and
                    // hash buckets so the row-count hint can keep a low-cardinality
                    // build resident instead of producing two expanded spill runs.
                    rows.checked_mul(2 * std::mem::size_of::<u128>())?
                        .checked_mul(8)?
                        .checked_div(7)
                })?;
                Some(bytes.max(MIN_ENCODED_INDEX_SORTER_BYTES))
            })
            .collect::<Option<Vec<_>>>();
        let estimated = program_estimates.as_ref().and_then(|estimates| {
            estimates
                .iter()
                .try_fold(0usize, |total, bytes| total.checked_add(*bytes))
        });
        let soft = crate::engine::executor::mem_budget::query_mem_limit_bytes().max(minimum);
        let requested_aggregate = estimated
            .unwrap_or(soft)
            .max(soft)
            .saturating_add(parallel_scratch)
            .saturating_add(if prepaid_scan {
                scan_degree * ENCODED_SCAN_BATCH_BYTES
            } else {
                0
            });
        let (_buffer_lease, mut charge, aggregate) =
            reserve_offline_index_build_scratch(self, requested_aggregate, minimum)?;
        INDEX_BUILD_SCRATCH_GRANTED_PEAK.fetch_max(aggregate as u64, Ordering::Relaxed);
        let scan_bytes_per_worker = if prepaid_scan {
            // Multiple disjoint 4 MiB reads already keep the direct-scan I/O
            // queue full. A larger prepaid batch mostly displaces sort runs
            // under a small memory target, multiplying the final merge work.
            ((aggregate - sorter_minimum - parallel_scratch) / scan_degree)
                .min(ENCODED_SCAN_BATCH_BYTES)
                / PAGE_SIZE
                * PAGE_SIZE
        } else {
            0
        };
        let scan_bytes = scan_bytes_per_worker * scan_degree;
        let mut scan_charges = (0..if prepaid_scan { scan_degree } else { 0 })
            .map(|_| charge.split(scan_bytes_per_worker))
            .collect::<Vec<_>>();
        let sorter_budget = aggregate - parallel_scratch - scan_bytes;
        // Weight each sorter by the capacity it actually allocates per row
        // (Fixed128 records occupy 16 bytes regardless of key width, and
        // variable sorters need a dictionary envelope). Weighting by encoded
        // key width instead under-provisioned narrow keys: a fully granted
        // multi-index build still spilled one near-complete run per narrow
        // index, sorted under the shared scan lock. Weights exclude the
        // per-sorter minimum, so a full grant gives every sorter exactly
        // its estimate.
        let weights = encoded_sorter_budget_weights(program_estimates.as_deref(), programs.len());
        let budgets = proportional_encoded_index_budgets(&weights, sorter_budget);
        // Sorting runs one index at a time at the full degree (see the
        // sort/write pipeline below), so every sorter gets the whole degree.
        let sorter_degree = options.effective_degree;
        let mut sorters = programs
            .iter()
            .cloned()
            .zip(budgets)
            .map(|(program, budget)| {
                EncodedIndexSorter::new(program, budget).with_sort_degree(sorter_degree)
            })
            .collect::<Vec<_>>();
        debug_assert!(
            sorters
                .iter()
                .map(EncodedIndexSorter::capacity_bytes)
                .sum::<usize>()
                <= aggregate
        );

        let scan_started = Instant::now();
        let mut visible_rows = 0u64;
        if scan_degree > 1 && fixed_parallel {
            let projections = programs
                .iter()
                .map(|program| {
                    program
                        .direct_parts
                        .as_ref()
                        .expect("parallel program has direct parts")
                        .iter()
                        .zip(
                            program
                                .fixed_ops
                                .as_ref()
                                .expect("parallel program has fixed opcodes"),
                        )
                        .map(|(part, op)| (part.value_offset, *op))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let strides = programs
                .iter()
                .map(|program| {
                    program
                        .fixed_record_width()
                        .expect("parallel program has fixed width")
                })
                .collect::<Vec<_>>();
            let shared_sorters = sorters.into_iter().map(Mutex::new).collect::<Vec<_>>();
            let next_page = AtomicU64::new(0);
            visible_rows = self.with_heap_read(table.id, |heap| {
                std::thread::scope(|scope| -> Result<u64, QueryError> {
                    let mut handles = Vec::with_capacity(scan_degree);
                    for worker in 0..scan_degree {
                        let shared_sorters = &shared_sorters;
                        let next_page = &next_page;
                        let projections = &projections;
                        let strides = &strides;
                        let needed_columns = &needed_columns;
                        let mut scan_charge = scan_charges.pop().expect("prepaid scan worker lease");
                        handles.push(
                            std::thread::Builder::new()
                                .name(format!("mydb-encoded-index-scan-{worker}"))
                                .stack_size(4 * 1024 * 1024)
                                .spawn_scoped(scope, move || -> Result<u64, QueryError> {
                                    let _worker_guard = enter_index_build_worker();
                                    let mut projected = vec![
                                        OfflineProjectedScalar::Missing;
                                        needed_columns.len()
                                    ];
                                    let mut value_fallback = Vec::with_capacity(needed_columns.len());
                                    let mut chunks = strides
                                        .iter()
                                        .map(|stride| {
                                            Vec::with_capacity(
                                                stride.saturating_mul(ENCODED_SCAN_CHUNK_ROWS),
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    let mut chunk_rows = 0usize;
                                    let mut visible = 0u64;
                                    let flush = |chunks: &mut [Vec<u8>]| {
                                        flush_chunks_to_sorters(
                                            shared_sorters,
                                            chunks,
                                            worker,
                                            Vec::is_empty,
                                            |sorter, chunk| {
                                                sorter.push_fixed_batch(self.data_dir(), chunk)?;
                                                chunk.clear();
                                                Ok(())
                                            },
                                        )
                                    };
                                    loop {
                                        let start_page =
                                            next_page.fetch_add(task_pages, Ordering::Relaxed);
                                        if start_page >= page_count {
                                            break;
                                        }
                                        let end_page = start_page
                                            .saturating_add(task_pages)
                                            .min(page_count);
                                        heap.scan_mvcc_payloads_range_with_charge_until(
                                            needed_columns,
                                            start_page,
                                            end_page,
                                            &mut scan_charge,
                                            online,
                                            |row| {
                                                if row.header.is_deleted() {
                                                    return Ok(false);
                                                }
                                                projected.fill(OfflineProjectedScalar::Missing);
                                                let mut fast = {
                                                    let mut sink = OfflineFixedDecodeSink {
                                                        values: &mut projected,
                                                    };
                                                    decode_row_into_sink_fast(
                                                        &row.payload[row.body_offset..],
                                                        row.codec,
                                                        row.layout_id,
                                                        row.plan,
                                                        &mut sink,
                                                        row.has_external,
                                                    )?
                                                };
                                                fast &= !projected.iter().any(|value| {
                                                    matches!(
                                                        value,
                                                        OfflineProjectedScalar::Unsupported
                                                            | OfflineProjectedScalar::Missing
                                                    )
                                                });
                                                if !fast {
                                                    value_fallback.clear();
                                                    if !HeapFile::decode_direct_payload_projection_into(
                                                        &row,
                                                        &mut value_fallback,
                                                    )? {
                                                        return Ok(false);
                                                    }
                                                    for (slot, value) in projected
                                                        .iter_mut()
                                                        .zip(value_fallback.iter())
                                                    {
                                                        *slot = projected_scalar_from_value(value)?;
                                                    }
                                                }
                                                for ((projection, stride), chunk) in projections
                                                    .iter()
                                                    .zip(strides)
                                                    .zip(chunks.iter_mut())
                                                {
                                                    let start = chunk.len();
                                                    for (offset, op) in projection {
                                                        encode_projected_fixed_value(
                                                            projected[*offset],
                                                            *op,
                                                            chunk,
                                                        )?;
                                                    }
                                                    chunk.extend_from_slice(
                                                        &row.row_id.page_id.to_be_bytes(),
                                                    );
                                                    chunk.extend_from_slice(
                                                        &row.row_id.slot_id.to_be_bytes(),
                                                    );
                                                    debug_assert_eq!(chunk.len() - start, *stride);
                                                }
                                                visible = visible.saturating_add(1);
                                                chunk_rows += 1;
                                                if chunk_rows >= ENCODED_SCAN_CHUNK_ROWS {
                                                    flush(&mut chunks)?;
                                                    chunk_rows = 0;
                                                }
                                                Ok(false)
                                            },
                                        )?;
                                    }
                                    if chunk_rows != 0 {
                                        flush(&mut chunks)?;
                                    }
                                    Ok(visible)
                                })
                                .map_err(|err| {
                                    error(
                                        crate::error::catalog::ER_GET_ERRNO,
                                        format!("failed to spawn encoded scan worker: {err}"),
                                    )
                                })?,
                        );
                    }
                    let mut visible = 0u64;
                    for handle in handles {
                        visible = visible.saturating_add(handle.join().map_err(|_| {
                            error(
                                crate::error::catalog::ER_UNKNOWN_ERROR,
                                "encoded index scan worker panicked",
                            )
                        })??);
                    }
                    Ok(visible)
                })
            })?;
            sorters = into_inner_sorters(shared_sorters)?;
            charge.shrink(parallel_scratch);
        } else if scan_degree > 1 {
            // Variable direct-column programs still benefit from disjoint
            // page-range decode/collation work. Workers build bounded encoded
            // chunks without the sorter lock; the short flush section only
            // interns/copies already-encoded keys into the governed sorter.
            let shared_sorters = sorters.into_iter().map(Mutex::new).collect::<Vec<_>>();
            let next_page = AtomicU64::new(0);
            visible_rows = self.with_heap_read(table.id, |heap| {
                std::thread::scope(|scope| -> Result<u64, QueryError> {
                    let mut handles = Vec::with_capacity(scan_degree);
                    for worker in 0..scan_degree {
                        let shared_sorters = &shared_sorters;
                        let next_page = &next_page;
                        let programs = &programs;
                        let needed_columns = &needed_columns;
                        handles.push(
                            std::thread::Builder::new()
                                .name(format!("mydb-encoded-variable-scan-{worker}"))
                                .stack_size(4 * 1024 * 1024)
                                .spawn_scoped(scope, move || -> Result<u64, QueryError> {
                                    let _worker_guard = enter_index_build_worker();
                                    let mut chunks = programs
                                        .iter()
                                        .map(|_| {
                                            Vec::<(Vec<EncodedPendingIndexRecord>, RowId)>::with_capacity(
                                                ENCODED_SCAN_CHUNK_ROWS,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    let mut chunk_rows = 0usize;
                                    let mut visible = 0u64;
                                    let flush = |chunks: &mut [Vec<(Vec<EncodedPendingIndexRecord>, RowId)>]| {
                                        flush_chunks_to_sorters(
                                            shared_sorters,
                                            chunks,
                                            worker,
                                            Vec::is_empty,
                                            |sorter, chunk| {
                                                for (records, row_id) in chunk.drain(..) {
                                                    for pending in records {
                                                        sorter.push(self.data_dir(), pending, row_id)?;
                                                    }
                                                }
                                                Ok(())
                                            },
                                        )
                                    };
                                    loop {
                                        let start_page =
                                            next_page.fetch_add(task_pages, Ordering::Relaxed);
                                        if start_page >= page_count {
                                            break;
                                        }
                                        let end_page = start_page
                                            .saturating_add(task_pages)
                                            .min(page_count);
                                        let visit = |row: ProjectedMvccStoredRow<'_>| {
                                                if row.header.is_deleted() {
                                                    return Ok(false);
                                                }
                                                for (program, chunk) in
                                                    programs.iter().zip(chunks.iter_mut())
                                                {
                                                    chunk.push((
                                                        program.encode_row(table, row.values)?,
                                                        row.row_id,
                                                    ));
                                                }
                                                visible = visible.saturating_add(1);
                                                chunk_rows += 1;
                                                if chunk_rows >= ENCODED_SCAN_CHUNK_ROWS {
                                                    flush(&mut chunks)?;
                                                    chunk_rows = 0;
                                                }
                                                Ok(false)
                                            };
                                        if online {
                                            heap.scan_mvcc_columns_range_until(needed_columns, start_page, end_page, visit)?;
                                        } else {
                                            heap.scan_mvcc_columns_direct_range_impl(needed_columns, start_page, end_page, visit)?;
                                        }
                                    }
                                    if chunk_rows != 0 {
                                        flush(&mut chunks)?;
                                    }
                                    Ok(visible)
                                })
                                .map_err(|err| {
                                    error(
                                        crate::error::catalog::ER_GET_ERRNO,
                                        format!("failed to spawn encoded variable scan worker: {err}"),
                                    )
                                })?,
                        );
                    }
                    let mut visible = 0u64;
                    for handle in handles {
                        visible = visible.saturating_add(handle.join().map_err(|_| {
                            error(
                                crate::error::catalog::ER_UNKNOWN_ERROR,
                                "encoded variable scan worker panicked",
                            )
                        })??);
                    }
                    Ok(visible)
                })
            })?;
            sorters = into_inner_sorters(shared_sorters)?;
            charge.shrink(parallel_scratch);
        } else {
            self.with_heap_read(table.id, |heap| {
                let visit = |row: ProjectedMvccStoredRow<'_>| {
                    if row.header.is_deleted() {
                        return Ok(false);
                    }
                    visible_rows = visible_rows.saturating_add(1);
                    for sorter in &mut sorters {
                        sorter.push_row(self.data_dir(), table, &row.values, row.row_id)?;
                    }
                    Ok(false)
                };
                if online {
                    heap.scan_mvcc_columns_until(&needed_columns, visit)
                } else {
                    heap.scan_mvcc_columns_direct_until(&needed_columns, visit)
                }
            })?;
        }
        INDEX_BUILD_SCAN_US.fetch_add(
            scan_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        INDEX_BUILD_ROWS.fetch_add(visible_rows, Ordering::Relaxed);
        INDEX_BUILD_HEAP_SCANS.fetch_add(1, Ordering::Relaxed);

        let validations = Mutex::new(HashMap::new());
        let write_one = |index: IndexSchema,
                         program: Arc<CompiledIndexKeyProgram>,
                         sorted: EncodedSortedRecords|
         -> Result<(), QueryError> {
            let path = self.index_path_for_table_schema(table, table.id, &index.name);
            let _generation = btree_logical_wal::LsnScope::new(Some(generation_lsn));
            let mut validation = validation_indexes
                .contains(&index.name)
                .then(IndexBuildValidation::default);
            let file = create_rebuilt_from_encoded_sorted(
                self,
                path,
                &index,
                program,
                sorted,
                &mut validation,
            )?;
            if let Some(mut validation) = validation {
                // Retain only counts, not an uncharged variable-width key.
                validation.previous = None;
                validations
                    .lock()
                    .unwrap()
                    .insert(index.name.clone(), validation);
            }
            file.flush()?;
            if !online {
                self.remove_index_cache_entry(table.id, &index.name)?;
            }
            Ok(())
        };
        // Sort indexes one at a time at the full degree and hand each sorted
        // index to a writer pool. Bulk B-tree writes are I/O-bound while
        // sorting is CPU-bound; sorting every index concurrently first left
        // the device idle until the slowest sort finished.
        let work = indexes
            .into_iter()
            .zip(programs)
            .zip(sorters)
            .map(|((index, program), sorter)| (index, program, sorter))
            .collect::<Vec<_>>();
        let outer_degree = options.effective_degree.min(work.len()).max(1);
        let (sender, receiver) = std::sync::mpsc::channel();
        let receiver = Mutex::new(receiver);
        let cancelled = AtomicBool::new(false);
        std::thread::scope(|scope| -> Result<(), QueryError> {
            let mut handles = Vec::with_capacity(outer_degree);
            for _ in 0..outer_degree {
                handles.push(scope.spawn(|| -> Result<(), QueryError> {
                    loop {
                        if cancelled.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        let item = receiver
                            .lock()
                            .map_err(|_| {
                                error(
                                    crate::error::catalog::ER_UNKNOWN_ERROR,
                                    "encoded index-build queue mutex poisoned",
                                )
                            })?
                            .recv();
                        let Ok((index, program, sorted)) = item else {
                            return Ok(());
                        };
                        if let Err(err) = write_one(index, program, sorted) {
                            cancelled.store(true, Ordering::Release);
                            return Err(err);
                        }
                    }
                }));
            }
            let sorted = (|| -> Result<(), QueryError> {
                for (index, program, sorter) in work {
                    if cancelled.load(Ordering::Acquire) {
                        break;
                    }
                    let sort_started = Instant::now();
                    let sorted = sorter.finish(self.data_dir())?;
                    INDEX_BUILD_SORT_US.fetch_add(
                        sort_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                        Ordering::Relaxed,
                    );
                    if sender.send((index, program, sorted)).is_err() {
                        break;
                    }
                }
                Ok(())
            })();
            if sorted.is_err() {
                cancelled.store(true, Ordering::Release);
            }
            drop(sender);
            let mut first_error = sorted.err();
            for handle in handles {
                let written = handle.join().map_err(|_| {
                    error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "encoded index-build worker panicked",
                    )
                })?;
                if let Err(err) = written {
                    first_error.get_or_insert(err);
                }
            }
            first_error.map_or(Ok(()), Err)
        })?;
        drop(charge);
        // Online callers already account for this build. Do not report a
        // second serial build merely because only one index was requested.
        if !online {
            let effective = scan_degree.max(outer_degree).max(sorter_degree);
            INDEX_BUILD_REQUESTED_WORKERS
                .fetch_add(options.requested_degree as u64, Ordering::Relaxed);
            INDEX_BUILD_EFFECTIVE_WORKERS.fetch_add(effective as u64, Ordering::Relaxed);
            if effective > 1 {
                INDEX_BUILD_PARALLEL_BUILDS.fetch_add(1, Ordering::Relaxed);
            } else {
                INDEX_BUILD_SERIAL_BUILDS.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(validations.into_inner().unwrap())
    }
}

impl CompiledIndexKeyProgram {
    pub(crate) fn compile(table: &TableSchema, index: &IndexSchema) -> Result<Self, QueryError> {
        let mut direct_parts = Vec::with_capacity(
            index
                .columns
                .len()
                .saturating_add(index.included_columns.len()),
        );
        let mut direct = !index.is_fulltext()
            && !index.is_spatial()
            && !index.is_vector()
            && !index.has_multi_valued_parts();
        if direct {
            for key_pos in 0..index.columns.len() {
                match index.key_part(key_pos) {
                    Some(IndexKeyPart::Column(name)) => direct_parts.push(CompiledIndexKeyPart {
                        source_column_idx: table.column_index(name)?,
                        value_offset: table.column_index(name)?,
                        key_pos,
                    }),
                    Some(IndexKeyPart::Expression(_)) | None => {
                        direct = false;
                        break;
                    }
                }
            }
        }
        if direct {
            for name in &index.included_columns {
                direct_parts.push(CompiledIndexKeyPart {
                    source_column_idx: table.column_index(name)?,
                    value_offset: table.column_index(name)?,
                    key_pos: direct_parts.len(),
                });
            }
        }
        let direct_parts = direct.then_some(direct_parts);
        let decode_schema = index
            .effective_columns()
            .iter()
            .enumerate()
            .map(|(pos, name)| {
                // Expression slots carry SQL text, not column names. Decode
                // the same canonical key type emitted by index_keys, including
                // CAST signedness and multi-valued expression element types.
                if let Some(expr) = index.key_expression(pos) {
                    return Ok(crate::storage::schema::index_expression_param_target(expr));
                }
                let column = &table.columns[table.column_index(name)?];
                Ok(ParamTarget::for_column_with_collation(
                    &column.data_type,
                    column.unsigned,
                    column.collation.as_deref(),
                ))
            })
            .collect::<Result<Vec<_>, QueryError>>()?;
        let fixed_ops = direct_parts.as_ref().and_then(|parts| {
            parts
                .iter()
                .map(|part| fixed_key_op_for_column(&table.columns[part.source_column_idx]))
                .collect::<Option<Vec<_>>>()
        });
        let fixed_signed_widths = fixed_ops.as_ref().and_then(|ops| {
            ops.iter()
                .map(|op| match op {
                    FixedKeyOp::Signed(width) => Some(*width),
                    _ => None,
                })
                .collect::<Option<smallvec::SmallVec<[u8; 4]>>>()
        });
        let physical_lane = direct_parts
            .as_ref()
            .filter(|parts| {
                parts
                    .iter()
                    .all(|part| !table.columns[part.source_column_idx].nullable)
            })
            .and_then(|_| fixed_ops.as_deref())
            .map_or(EncodedPhysicalLane::Generic, |ops| match ops {
                [FixedKeyOp::Signed(_)] => EncodedPhysicalLane::SingleInt,
                [FixedKeyOp::Signed(_), FixedKeyOp::Signed(_)] => EncodedPhysicalLane::PairInt,
                [FixedKeyOp::Date] => EncodedPhysicalLane::Date,
                _ => EncodedPhysicalLane::Generic,
            });
        Ok(Self {
            index: index.clone(),
            direct_parts,
            decode_schema,
            fixed_ops,
            fixed_signed_widths,
            physical_lane,
        })
    }

    fn with_projection(&self, offsets: &HashMap<usize, usize>) -> Option<Self> {
        let mut projected = self.clone();
        let parts = projected.direct_parts.as_mut()?;
        for part in parts {
            part.value_offset = *offsets.get(&part.source_column_idx)?;
        }
        Some(projected)
    }

    fn source_columns(&self) -> Option<impl Iterator<Item = usize> + '_> {
        self.direct_parts
            .as_ref()
            .map(|parts| parts.iter().map(|part| part.source_column_idx))
    }

    pub(crate) fn encode_row(
        &self,
        table: &TableSchema,
        values: &[Value],
    ) -> Result<Vec<EncodedPendingIndexRecord>, QueryError> {
        if let (Some(parts), Some(ops)) = (&self.direct_parts, &self.fixed_ops) {
            let mut order_key = smallvec::SmallVec::<[u8; 32]>::with_capacity(
                ops.iter().map(|op| op.width()).sum(),
            );
            let mut has_null = false;
            for (part, op) in parts.iter().zip(ops) {
                let value = &values[part.value_offset];
                has_null |= matches!(value, Value::Null);
                encode_fixed_value(value, *op, &mut order_key)?;
            }
            return Ok(vec![EncodedPendingIndexRecord {
                order_key,
                materialized_key: None,
                has_null,
            }]);
        }
        let keys = if let Some(parts) = &self.direct_parts {
            let mut key = smallvec::SmallVec::<[KeyPart; 4]>::with_capacity(parts.len());
            for part in parts {
                let column = &table.columns[part.source_column_idx];
                let encoded = KeyPart::from_column_value_with_collation(
                    &values[part.value_offset],
                    &column.data_type,
                    column.collation.as_deref(),
                );
                key.push(self.index.apply_prefix_to_key_part(part.key_pos, encoded));
            }
            vec![IndexKey::from(key)]
        } else {
            // Expression, prefix-expression and MVI remain on the semantic
            // oracle, but their produced keys immediately enter the same
            // encoded record pipeline as direct columns.
            table.index_keys(&self.index, values)?
        };
        keys.iter().map(|key| self.encode_key(key)).collect()
    }

    pub(crate) fn encode_fixed_values_into(
        &self,
        values: &[Value],
        out: &mut Vec<u8>,
    ) -> Result<(), QueryError> {
        let parts = self
            .direct_parts
            .as_ref()
            .expect("fixed program has direct parts");
        let ops = self.fixed_ops.as_ref().expect("fixed program has opcodes");
        for (part, op) in parts.iter().zip(ops) {
            encode_fixed_value_vec(&values[part.value_offset], *op, out)?;
        }
        Ok(())
    }

    pub(crate) fn fixed_key_width(&self) -> Option<usize> {
        self.fixed_record_width().map(|width| width - 6)
    }

    fn encode_key(&self, key: &IndexKey) -> Result<EncodedPendingIndexRecord, QueryError> {
        if key.0.len() != self.decode_schema.len() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "compiled index key arity does not match emitted key",
            ));
        }
        let order_key: smallvec::SmallVec<[u8; 32]> = if let Some(ops) = &self.fixed_ops {
            encode_fixed_order_key(key, ops)?
        } else {
            smallvec::SmallVec::from_vec(encode_key_memcmp(key))
        };
        let reversible = !key
            .0
            .iter()
            .any(|part| matches!(part, KeyPart::CollatedString { .. }));
        let materialized_key = if reversible {
            None
        } else {
            let mut payload = Vec::new();
            encode_index_key_into(key, &mut payload)?;
            Some(payload)
        };
        Ok(EncodedPendingIndexRecord {
            order_key,
            materialized_key,
            has_null: key.0.iter().any(|part| matches!(part, KeyPart::Null)),
        })
    }

    fn attach_row_id(
        &self,
        record: EncodedPendingIndexRecord,
        row_id: RowId,
    ) -> EncodedIndexRecord {
        EncodedIndexRecord {
            order_key: record.order_key.into_vec(),
            materialized_key: record.materialized_key,
            row_id,
            has_null: record.has_null,
        }
    }

    fn materialize_key(&self, record: &EncodedIndexRecord) -> Result<IndexKey, QueryError> {
        if let Some(payload) = &record.materialized_key {
            let mut offset = 0usize;
            let key = decode_index_key_from(payload, &mut offset)?;
            if offset != payload.len() {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "encoded index materialization payload has trailing bytes",
                ));
            }
            return Ok(key);
        }
        if self.fixed_ops.is_some() {
            return decode_compiled_fixed_order_key(&record.order_key, self);
        }
        decode_key_memcmp(&record.order_key, &self.decode_schema).map_err(|message| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("invalid encoded index order key: {message}"),
            )
        })
    }

    fn into_index_entry(&self, record: EncodedIndexRecord) -> Result<IndexEntry, QueryError> {
        let key = self.materialize_key(&record)?;
        Ok(IndexEntry {
            key,
            row_id: record.row_id,
        })
    }

    pub(crate) fn fixed_record_width(&self) -> Option<usize> {
        self.fixed_ops.as_ref().map(|ops| {
            ops.iter()
                .map(|op| op.width())
                .sum::<usize>()
                .saturating_add(6)
        })
    }
}

fn decode_compiled_fixed_order_key(
    bytes: &[u8],
    program: &CompiledIndexKeyProgram,
) -> Result<IndexKey, QueryError> {
    if let Some(widths) = &program.fixed_signed_widths {
        let expected = widths
            .iter()
            .map(|width| 1 + usize::from(*width))
            .sum::<usize>();
        if bytes.len() != expected {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "fixed signed index order key has invalid width",
            ));
        }
        let mut parts = smallvec::SmallVec::<[KeyPart; 4]>::with_capacity(widths.len());
        let mut offset = 0usize;
        for width in widths {
            if bytes[offset] == 0 {
                parts.push(KeyPart::Null);
            } else {
                parts.push(KeyPart::Int(decode_fixed_signed(
                    &bytes[offset..offset + 1 + usize::from(*width)],
                    *width,
                )));
            }
            offset += 1 + usize::from(*width);
        }
        return Ok(IndexKey::from(parts));
    }
    decode_fixed_order_key(bytes, program.fixed_ops.as_deref().expect("fixed opcodes"))
}

fn encode_fixed_value(
    value: &Value,
    op: FixedKeyOp,
    out: &mut smallvec::SmallVec<[u8; 32]>,
) -> Result<(), QueryError> {
    if matches!(value, Value::Null) {
        out.resize(out.len() + op.width(), 0);
        return Ok(());
    }
    out.push(1);
    match (op, value) {
        (FixedKeyOp::Signed(bytes), Value::Int(value)) => {
            let bits = u32::from(bytes) * 8;
            let biased = if bits == 64 {
                (*value as u64) ^ (1_u64 << 63)
            } else {
                value.wrapping_add(1_i64 << (bits - 1)) as u64
            };
            let encoded = biased.to_be_bytes();
            out.extend_from_slice(&encoded[8 - usize::from(bytes)..]);
        }
        (FixedKeyOp::Unsigned(bytes), Value::UInt(value)) => {
            let encoded = value.to_be_bytes();
            out.extend_from_slice(&encoded[8 - usize::from(bytes)..]);
        }
        (FixedKeyOp::Float32, Value::Float(value)) => {
            out.extend_from_slice(&f32_to_order_u32(*value).to_be_bytes())
        }
        (FixedKeyOp::Float64, Value::Double(value)) => {
            out.extend_from_slice(&f64_to_order_u64(*value).to_be_bytes())
        }
        (FixedKeyOp::DecimalI128 { scale }, Value::DecimalCompact(value, value_scale)) => {
            if *value_scale != scale {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "compiled decimal index scale mismatch",
                ));
            }
            out.extend_from_slice(&((i128::from(*value) as u128) ^ (1_u128 << 127)).to_be_bytes());
            out.push(scale);
        }
        (FixedKeyOp::DecimalI128 { scale }, Value::Decimal128(value, value_scale)) => {
            if *value_scale != scale {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "compiled decimal index scale mismatch",
                ));
            }
            out.extend_from_slice(&((*value as u128) ^ (1_u128 << 127)).to_be_bytes());
            out.push(scale);
        }
        (FixedKeyOp::DecimalI128 { scale }, Value::Decimal(text)) => {
            let (value, actual_scale) = crate::util::decimal::parse_exact_decimal_str(text)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "invalid stored decimal key",
                    )
                })?;
            let value = crate::util::decimal::rescale_round_i128(value, actual_scale, scale)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "stored decimal key exceeds encoded width",
                    )
                })?;
            out.extend_from_slice(&((value as u128) ^ (1_u128 << 127)).to_be_bytes());
            out.push(scale);
        }
        (FixedKeyOp::DecimalWide { scale }, Value::DecimalWide(value)) => {
            if value.scale != scale {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "compiled wide-decimal index scale mismatch",
                ));
            }
            let signed = value.magnitude.to_signed_bytes_be();
            let fill = if value.magnitude.sign() == num_bigint::Sign::Minus {
                0xff
            } else {
                0
            };
            let mut fixed = [fill; 28];
            fixed[28 - signed.len()..].copy_from_slice(&signed);
            fixed[0] ^= 0x80;
            out.extend_from_slice(&fixed);
            out.push(scale);
        }
        (FixedKeyOp::Date, Value::Date(value)) => {
            out.extend_from_slice(&((*value as u32) ^ (1_u32 << 31)).to_be_bytes())
        }
        (FixedKeyOp::Time, Value::Time(value)) => {
            out.extend_from_slice(&((*value as u64) ^ (1_u64 << 63)).to_be_bytes())
        }
        (FixedKeyOp::DateTime, Value::DateTime(value) | Value::Timestamp(value)) => {
            out.extend_from_slice(&((*value as u64) ^ (1_u64 << 63)).to_be_bytes())
        }
        (FixedKeyOp::Year, Value::Year(value)) => out.push(*value),
        _ => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "encoded index value does not match compiled column type",
            ));
        }
    }
    Ok(())
}

fn encode_fixed_value_vec(
    value: &Value,
    op: FixedKeyOp,
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    let mut encoded = smallvec::SmallVec::<[u8; 32]>::new();
    encode_fixed_value(value, op, &mut encoded)?;
    out.extend_from_slice(&encoded);
    Ok(())
}

fn encode_projected_fixed_value(
    value: OfflineProjectedScalar,
    op: FixedKeyOp,
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    if matches!(value, OfflineProjectedScalar::Null) {
        out.resize(out.len() + op.width(), 0);
        return Ok(());
    }
    out.push(1);
    match (op, value) {
        (FixedKeyOp::Signed(bytes), OfflineProjectedScalar::Signed(value)) => {
            let bits = u32::from(bytes) * 8;
            let biased = if bits == 64 {
                (value as u64) ^ (1_u64 << 63)
            } else {
                value.wrapping_add(1_i64 << (bits - 1)) as u64
            };
            let encoded = biased.to_be_bytes();
            out.extend_from_slice(&encoded[8 - usize::from(bytes)..]);
        }
        (FixedKeyOp::Unsigned(bytes), OfflineProjectedScalar::Unsigned(value)) => {
            let encoded = value.to_be_bytes();
            out.extend_from_slice(&encoded[8 - usize::from(bytes)..]);
        }
        (FixedKeyOp::Float32, OfflineProjectedScalar::Float32(value)) => {
            out.extend_from_slice(&f32_to_order_u32(value).to_be_bytes());
        }
        (FixedKeyOp::Float64, OfflineProjectedScalar::Float64(value)) => {
            out.extend_from_slice(&f64_to_order_u64(value).to_be_bytes());
        }
        (FixedKeyOp::DecimalI128 { scale }, OfflineProjectedScalar::Decimal(value, actual))
            if scale == actual =>
        {
            out.extend_from_slice(&((value as u128) ^ (1_u128 << 127)).to_be_bytes());
            out.push(scale);
        }
        (FixedKeyOp::Date, OfflineProjectedScalar::Date(value)) => {
            out.extend_from_slice(&((value as u32) ^ (1_u32 << 31)).to_be_bytes());
        }
        (FixedKeyOp::Time, OfflineProjectedScalar::Time(value)) => {
            out.extend_from_slice(&((value as u64) ^ (1_u64 << 63)).to_be_bytes());
        }
        (FixedKeyOp::DateTime, OfflineProjectedScalar::DateTime(value)) => {
            out.extend_from_slice(&((value as u64) ^ (1_u64 << 63)).to_be_bytes());
        }
        (FixedKeyOp::Year, OfflineProjectedScalar::Year(value)) => out.push(value),
        _ => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "projected heap scalar does not match compiled index opcode",
            ));
        }
    }
    Ok(())
}

fn projected_scalar_from_value(value: &Value) -> Result<OfflineProjectedScalar, QueryError> {
    Ok(match value {
        Value::Null => OfflineProjectedScalar::Null,
        Value::Int(value) => OfflineProjectedScalar::Signed(*value),
        Value::UInt(value) => OfflineProjectedScalar::Unsigned(*value),
        Value::Float(value) => OfflineProjectedScalar::Float32(*value),
        Value::Double(value) => OfflineProjectedScalar::Float64(*value),
        Value::DecimalCompact(value, scale) => {
            OfflineProjectedScalar::Decimal(i128::from(*value), *scale)
        }
        Value::Decimal128(value, scale) => OfflineProjectedScalar::Decimal(*value, *scale),
        Value::Decimal(text) => {
            let (value, scale) =
                crate::util::decimal::parse_exact_decimal_str(text).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "invalid stored decimal key",
                    )
                })?;
            OfflineProjectedScalar::Decimal(value, scale)
        }
        Value::Date(value) => OfflineProjectedScalar::Date(*value),
        Value::Time(value) => OfflineProjectedScalar::Time(*value),
        Value::DateTime(value) | Value::Timestamp(value) => {
            OfflineProjectedScalar::DateTime(*value)
        }
        Value::Year(value) => OfflineProjectedScalar::Year(*value),
        _ => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "projected heap value is not supported by the encoded index program",
            ));
        }
    })
}

fn fixed_key_op_for_column(column: &ColumnSchema) -> Option<FixedKeyOp> {
    use DataTypeKind::*;
    Some(match column.data_type {
        TinyInt | Bool => {
            if column.unsigned {
                FixedKeyOp::Unsigned(1)
            } else {
                FixedKeyOp::Signed(1)
            }
        }
        SmallInt => {
            if column.unsigned {
                FixedKeyOp::Unsigned(2)
            } else {
                FixedKeyOp::Signed(2)
            }
        }
        MediumInt => {
            if column.unsigned {
                FixedKeyOp::Unsigned(3)
            } else {
                FixedKeyOp::Signed(3)
            }
        }
        Int => {
            if column.unsigned {
                FixedKeyOp::Unsigned(4)
            } else {
                FixedKeyOp::Signed(4)
            }
        }
        BigInt => {
            if column.unsigned {
                FixedKeyOp::Unsigned(8)
            } else {
                FixedKeyOp::Signed(8)
            }
        }
        Bit { len } => FixedKeyOp::Unsigned(len.unwrap_or(1).div_ceil(8).clamp(1, 8) as u8),
        Float { .. } => FixedKeyOp::Float32,
        Double { .. } => FixedKeyOp::Float64,
        Decimal { precision, scale } if precision.unwrap_or(10) <= 38 => FixedKeyOp::DecimalI128 {
            scale: scale.unwrap_or(0),
        },
        Decimal { scale, .. } => FixedKeyOp::DecimalWide {
            scale: scale.unwrap_or(0),
        },
        Date => FixedKeyOp::Date,
        Time { .. } => FixedKeyOp::Time,
        DateTime { .. } | Timestamp { .. } => FixedKeyOp::DateTime,
        Year => FixedKeyOp::Year,
        Binary { .. }
        | VarBinary { .. }
        | Blob { .. }
        | Char { .. }
        | VarChar { .. }
        | Text { .. }
        | Json
        | Geometry { .. }
        | Vector { .. }
        | Enum { .. }
        | Set { .. } => return None,
    })
}

fn encode_fixed_order_key(
    key: &IndexKey,
    ops: &[FixedKeyOp],
) -> Result<smallvec::SmallVec<[u8; 32]>, QueryError> {
    let mut out = smallvec::SmallVec::with_capacity(ops.iter().map(|op| op.width()).sum());
    for (part, &op) in key.0.iter().zip(ops) {
        if matches!(part, KeyPart::Null) {
            out.resize(out.len() + op.width(), 0);
            continue;
        }
        out.push(1);
        match (op, part) {
            (FixedKeyOp::Signed(bytes), KeyPart::Int(value)) => {
                let bits = u32::from(bytes) * 8;
                let biased = i128::from(*value) + (1_i128 << (bits - 1));
                let encoded = (biased as u128).to_be_bytes();
                out.extend_from_slice(&encoded[16 - usize::from(bytes)..]);
            }
            (FixedKeyOp::Unsigned(bytes), KeyPart::UInt(value)) => {
                let encoded = value.to_be_bytes();
                out.extend_from_slice(&encoded[8 - usize::from(bytes)..]);
            }
            (FixedKeyOp::Float32, KeyPart::Float32(value)) => {
                out.extend_from_slice(&value.to_be_bytes())
            }
            (FixedKeyOp::Float64, KeyPart::Float64(value)) => {
                out.extend_from_slice(&value.to_be_bytes())
            }
            (FixedKeyOp::DecimalI128 { .. }, KeyPart::Decimal { value, scale }) => {
                out.extend_from_slice(&((*value as u128) ^ (1_u128 << 127)).to_be_bytes());
                out.push(*scale);
            }
            (FixedKeyOp::DecimalWide { .. }, KeyPart::DecimalWide { value, scale }) => {
                let signed = value.to_signed_bytes_be();
                let fill = if value.sign() == num_bigint::Sign::Minus {
                    0xff
                } else {
                    0
                };
                let mut fixed = [fill; 28];
                fixed[28 - signed.len()..].copy_from_slice(&signed);
                fixed[0] ^= 0x80;
                out.extend_from_slice(&fixed);
                out.push(*scale);
            }
            (FixedKeyOp::Date, KeyPart::Date(value)) => {
                out.extend_from_slice(&((*value as u32) ^ (1_u32 << 31)).to_be_bytes())
            }
            (FixedKeyOp::Time, KeyPart::Time(value))
            | (FixedKeyOp::DateTime, KeyPart::DateTime(value)) => {
                out.extend_from_slice(&((*value as u64) ^ (1_u64 << 63)).to_be_bytes())
            }
            (FixedKeyOp::Year, KeyPart::Year(value)) => out.push(*value),
            _ => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "encoded index key value does not match compiled schema",
                ));
            }
        }
    }
    Ok(out)
}

fn decode_fixed_order_key(bytes: &[u8], ops: &[FixedKeyOp]) -> Result<IndexKey, QueryError> {
    let expected = ops.iter().map(|op| op.width()).sum::<usize>();
    if bytes.len() != expected {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "encoded index order key has invalid width",
        ));
    }
    let mut parts = smallvec::SmallVec::<[KeyPart; 4]>::with_capacity(ops.len());
    let mut offset = 0usize;
    for &op in ops {
        let width = op.width();
        let encoded = &bytes[offset + 1..offset + width];
        if bytes[offset] == 0 {
            parts.push(KeyPart::Null);
            offset += width;
            continue;
        }
        let part = match op {
            FixedKeyOp::Signed(count) => {
                let mut full = [0u8; 16];
                full[16 - usize::from(count)..].copy_from_slice(encoded);
                let biased = u128::from_be_bytes(full) as i128;
                let bits = u32::from(count) * 8;
                KeyPart::Int((biased - (1_i128 << (bits - 1))) as i64)
            }
            FixedKeyOp::Unsigned(count) => {
                let mut full = [0u8; 8];
                full[8 - usize::from(count)..].copy_from_slice(encoded);
                KeyPart::UInt(u64::from_be_bytes(full))
            }
            FixedKeyOp::Float32 => {
                KeyPart::Float32(u32::from_be_bytes(encoded.try_into().unwrap()))
            }
            FixedKeyOp::Float64 => {
                KeyPart::Float64(u64::from_be_bytes(encoded.try_into().unwrap()))
            }
            FixedKeyOp::DecimalI128 { scale } => {
                let value = (u128::from_be_bytes(encoded[..16].try_into().unwrap())
                    ^ (1_u128 << 127)) as i128;
                KeyPart::Decimal { value, scale }
            }
            FixedKeyOp::DecimalWide { scale } => {
                let mut signed: [u8; 28] = encoded[..28].try_into().unwrap();
                signed[0] ^= 0x80;
                KeyPart::DecimalWide {
                    value: num_bigint::BigInt::from_signed_bytes_be(&signed),
                    scale,
                }
            }
            FixedKeyOp::Date => KeyPart::Date(
                (u32::from_be_bytes(encoded.try_into().unwrap()) ^ (1_u32 << 31)) as i32,
            ),
            FixedKeyOp::Time => KeyPart::Time(
                (u64::from_be_bytes(encoded.try_into().unwrap()) ^ (1_u64 << 63)) as i64,
            ),
            FixedKeyOp::DateTime => KeyPart::DateTime(
                (u64::from_be_bytes(encoded.try_into().unwrap()) ^ (1_u64 << 63)) as i64,
            ),
            FixedKeyOp::Year => KeyPart::Year(encoded[0]),
        };
        parts.push(part);
        offset += width;
    }
    Ok(IndexKey::from(parts))
}

#[cfg(test)]
mod encoded_index_program_tests {
    #[test]
    fn encoded_leaf_transcode_matches_canonical_codec_for_all_fixed_types() {
        let mut cases = Vec::new();
        for width in 1..=8 {
            let bits = width as u32 * 8;
            let lo = if width == 8 {
                i64::MIN
            } else {
                -(1i64 << (bits - 1))
            };
            let hi = if width == 8 {
                i64::MAX
            } else {
                (1i64 << (bits - 1)) - 1
            };
            for value in [lo, -1, 0, 1, hi] {
                cases.push((FixedKeyOp::Signed(width), KeyPart::Int(value)));
            }
            let hi = if width == 8 {
                u64::MAX
            } else {
                (1u64 << bits) - 1
            };
            for value in [0, 1, hi] {
                cases.push((FixedKeyOp::Unsigned(width), KeyPart::UInt(value)));
            }
        }
        for value in [0, 1, u32::MAX, 0x80000000] {
            cases.push((FixedKeyOp::Float32, KeyPart::Float32(value)));
        }
        for value in [0, 1, u64::MAX, 0x8000000000000000] {
            cases.push((FixedKeyOp::Float64, KeyPart::Float64(value)));
        }
        for value in [i128::MIN, -1, 0, i128::MAX] {
            cases.push((
                FixedKeyOp::DecimalI128 { scale: 8 },
                KeyPart::Decimal { value, scale: 8 },
            ));
        }
        for value in [-65537, -256, -129, -128, -1, 0, 127, 128, 255, 256] {
            cases.push((
                FixedKeyOp::DecimalWide { scale: 12 },
                KeyPart::DecimalWide {
                    value: value.into(),
                    scale: 12,
                },
            ));
        }
        for sign in [-1, 1] {
            cases.push((
                FixedKeyOp::DecimalWide { scale: 12 },
                KeyPart::DecimalWide {
                    value: (num_bigint::BigInt::from(sign) << 215) + 3,
                    scale: 12,
                },
            ));
        }
        for value in [i32::MIN, -1, 0, i32::MAX] {
            cases.push((FixedKeyOp::Date, KeyPart::Date(value)));
        }
        for value in [i64::MIN, -1, 0, i64::MAX] {
            cases.push((FixedKeyOp::Time, KeyPart::Time(value)));
            cases.push((FixedKeyOp::DateTime, KeyPart::DateTime(value)));
        }
        for value in [0, 1, 255] {
            cases.push((FixedKeyOp::Year, KeyPart::Year(value)));
        }
        for (op, part) in cases {
            for first in [part, KeyPart::Null] {
                let ops = [op, FixedKeyOp::Signed(8), FixedKeyOp::Unsigned(8)];
                let key = IndexKey::from(vec![first, KeyPart::Int(-11), KeyPart::UInt(u64::MAX)]);
                let order = encode_fixed_order_key(&key, &ops).unwrap();
                let mut actual = Vec::new();
                encode_fixed_leaf_key(&order, &ops, &mut actual).unwrap();
                let mut expected = vec![0; PAGE_SIZE];
                let mut offset = 0;
                encode_index_key_into_leaf_page(&key, &mut expected, &mut offset).unwrap();
                assert_eq!(actual, expected[..offset], "opcode {op:?}");
            }
        }
    }

    #[test]
    fn encoded_leaf_pages_are_byte_identical_to_legacy_codec() {
        let mut builder = EncodedLeafPageBuilder::new();
        for next in [Some(99), None] {
            let entries = (0..80)
                .map(|i| IndexEntry {
                    key: IndexKey::from(vec![
                        KeyPart::Int(i - 40),
                        KeyPart::String(format!("key-{i}")),
                        KeyPart::Null,
                    ]),
                    row_id: RowId {
                        page_id: 100_000 + i as u32,
                        slot_id: i as u16,
                    },
                })
                .collect::<Vec<_>>();
            for entry in &entries {
                let mut bytes = vec![0; PAGE_SIZE];
                let mut offset = 0;
                encode_index_key_into_leaf_page(&entry.key, &mut bytes, &mut offset).unwrap();
                builder.push(&bytes[..offset], entry.row_id).unwrap();
            }
            builder.first.take();
            let actual = builder.finish(next, 987654).unwrap();
            let mut expected =
                encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new(entries, next)))
                    .unwrap();
            set_page_lsn_if_formatted(&mut expected, 987654);
            assert_eq!(actual, expected);
            decode_index_tree_page(&actual).unwrap();
        }
    }

    #[test]
    fn encoded_composite_build_resident_and_spilled_counts_and_reopen() {
        for budget in [64 * 1024, 8 * 1024 * 1024] {
            let dir = tempfile::tempdir().unwrap();
            let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
            let (program, _) = record_for(
                vec![ParamTarget::Int; 3],
                IndexKey::from(vec![KeyPart::Int(0); 3]),
                RowId {
                    page_id: 0,
                    slot_id: 0,
                },
            );
            let program = Arc::new(program);
            let mut sorter = EncodedIndexSorter::new(program.clone(), budget);
            let mut expected = Vec::new();
            for i in (0..40_000u32).rev() {
                let group = i / 4;
                let key = IndexKey::from(vec![
                    if group % 7 == 0 {
                        KeyPart::Null
                    } else {
                        KeyPart::Int(group as i64 % 31)
                    },
                    KeyPart::Int(group as i64 % 17),
                    KeyPart::Int(group as i64),
                ]);
                let row_id = RowId {
                    page_id: i + 1,
                    slot_id: (i % 200) as u16,
                };
                sorter
                    .push(dir.path(), program.encode_key(&key).unwrap(), row_id)
                    .unwrap();
                expected.push((key, row_id));
            }
            expected.sort();
            let sorted = sorter.finish(dir.path()).unwrap();
            assert_eq!(
                matches!(&sorted, EncodedSortedRecords::Spilled { .. }),
                budget == 64 * 1024
            );
            let path = dir.path().join("encoded-composite.idx");
            let mut validation = Some(IndexBuildValidation::default());
            let file = create_rebuilt_from_encoded_composite(
                &db,
                path.clone(),
                &dummy_index(),
                program,
                sorted,
                &mut validation,
            )
            .unwrap();
            let validation = validation.unwrap();
            assert_eq!(validation.null_key_groups, 1429);
            assert_eq!(validation.duplicate_key_groups, 8571);
            assert_eq!(
                file.range_entries_chunk(None, None, None, 50_000).unwrap(),
                expected
            );
            drop(file);
            db.buffer_pool
                .discard_file(&Arc::new(BufferPool::index_file_key(path.clone())))
                .unwrap();
            let reopened = IndexFile::open_with_durability(
                path,
                db.log.clone(),
                DurabilityMode::Strict,
                db.buffer_pool.clone(),
            )
            .unwrap();
            assert_eq!(
                reopened
                    .range_entries_chunk(None, None, None, 50_000)
                    .unwrap(),
                expected
            );
        }
    }
    #[test]
    fn scan_tasks_distribute_small_heaps_and_bound_large_reads() {
        assert_eq!(super::encoded_scan_task_pages(0, 16), 32);
        assert_eq!(super::encoded_scan_task_pages(4096, 16), 64);
        assert_eq!(super::encoded_scan_task_pages(1_000_000, 16), 4096);
        assert_eq!(super::encoded_scan_task_pages(128, 1), 32);
    }

    use super::*;

    #[test]
    fn encoded_merge_bounds_readers_and_rejects_truncated_headers() {
        let root = tempfile::tempdir().unwrap();
        let tmp = IndexSortTmpDir::create(root.path()).unwrap();
        let tmp_path = tmp.path.clone();
        let mut runs = Vec::new();
        // More than one merge pass, overlapping keys and duplicate records.
        for run in 0..257 {
            let path = tmp.path.join(format!("input-{run}"));
            let mut writer = EncodedIndexRunCodec::create_writer(&path).unwrap();
            for value in [0u8, 1, 2] {
                EncodedIndexRunCodec::append(
                    &mut writer,
                    &EncodedIndexRecord {
                        order_key: vec![value],
                        materialized_key: Some(vec![value, 42]),
                        row_id: RowId {
                            page_id: value as u32,
                            slot_id: 0,
                        },
                        has_null: value == 0,
                    },
                )
                .unwrap();
            }
            EncodedIndexRunCodec::finish(writer).unwrap();
            runs.push(path);
        }
        let mut merger = EncodedRunMerger::open(tmp, runs).unwrap();
        assert!(merger.readers.len() <= 16);
        let records = merger.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 3);
        for (value, record) in records.iter().enumerate() {
            assert_eq!(record.order_key, vec![value as u8]);
            assert_eq!(record.materialized_key, Some(vec![value as u8, 42]));
            assert_eq!(record.has_null, value == 0);
        }
        drop(merger);
        assert!(!tmp_path.exists());

        for length in 1..15 {
            let path = root.path().join(format!("truncated-{length}"));
            fs::write(&path, vec![0; length]).unwrap();
            let mut reader = EncodedRunReader::open(&path).unwrap();
            assert!(reader.next_record().is_err());
        }
        let path = root.path().join("empty");
        fs::write(&path, []).unwrap();
        assert!(EncodedRunReader::open(&path)
            .unwrap()
            .next_record()
            .unwrap()
            .is_none());
    }

    #[test]
    fn fixed128_runs_merge_without_expanding_records_and_detect_truncation() {
        let root = tempfile::tempdir().unwrap();
        let tmp = IndexSortTmpDir::create(root.path()).unwrap();
        let mut runs = Vec::new();
        for run in 0..17 {
            let path = tmp.path.join(format!("fixed-{run}"));
            let mut writer = EncodedFixed128RunCodec::create_writer(&path).unwrap();
            for key in [0u128, 1, 2] {
                EncodedFixed128RunCodec::append(&mut writer, &key).unwrap();
            }
            EncodedFixed128RunCodec::finish(writer).unwrap();
            assert_eq!(fs::metadata(&path).unwrap().len(), 3 * 16);
            runs.push(path);
        }
        let merged = EncodedFixed128RunMerger::open(tmp, runs)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(merged, vec![0, 1, 2]);

        for length in 1..16 {
            let path = root.path().join(format!("fixed-truncated-{length}"));
            fs::write(&path, vec![0; length]).unwrap();
            assert!(EncodedFixed128RunReader::open(&path)
                .unwrap()
                .next_record()
                .is_err());
        }
    }

    #[test]
    fn fixed128_in_place_parallel_sort_preserves_total_order_and_dedup() {
        let source = (0..600_000u128)
            .flat_map(|value| {
                let record = ((value * 8191) % 131_071) << 48 | (value / 3);
                [record, record]
            })
            .collect::<Vec<_>>();
        let mut expected = source.clone();
        expected.sort_unstable();
        expected.dedup();
        for degree in [1, 2, 3, 16] {
            let mut records = source.clone();
            parallel_sort_fixed128_in_place(&mut records, degree);
            assert_eq!(records, expected, "degree={degree}");
        }
    }

    #[test]
    fn fixed128_partition_sort_handles_sparse_skewed_and_constant_inputs() {
        let rows = 1_200_000u128;
        let inputs: Vec<(&str, Vec<u128>)> = vec![
            // Varying bits separated by a long constant run: the bucket bits
            // must be gathered past the constant gap.
            (
                "sparse",
                (0..rows)
                    .map(|v| ((v % 3) << 120) | (0xABCD << 64) | ((v * 7919) % rows))
                    .collect(),
            ),
            // One key value holds almost every record.
            (
                "skewed",
                (0..rows)
                    .map(|v| {
                        let key = if v % 100 == 0 { v % 17 } else { 5 };
                        (key << 80) | ((rows - v) << 8)
                    })
                    .collect(),
            ),
            // Fewer varying bits than partition bits, with duplicates.
            (
                "narrow",
                (0..rows).map(|v| ((v * 13) % 64) << 100).collect(),
            ),
            ("constant", vec![42u128 << 70; rows as usize]),
        ];
        for (name, source) in inputs {
            let mut expected = source.clone();
            expected.sort_unstable();
            expected.dedup();
            for degree in [2, 7, 16] {
                let mut records = source.clone();
                assert!(
                    parallel_partition_sort_fixed128(&mut records, degree),
                    "{name} degree={degree} should use the partition sort"
                );
                assert_eq!(records, expected, "{name} degree={degree}");
            }
        }
        let mut small = vec![3u128, 1, 2];
        assert!(!parallel_partition_sort_fixed128(&mut small, 16));
        assert_eq!(small, [3, 1, 2]);
    }

    #[test]
    fn encoded_sorter_budgets_cover_each_estimate_regardless_of_key_width() {
        // Every Fixed128 sorter stores 16-byte records whatever the encoded
        // key width; a variable sorter needs its dictionary envelope. A full
        // grant must cover every estimate so no sorter spills a near-complete
        // run while its siblings keep unused capacity.
        let rows = 60_000_000usize;
        let estimates = [
            rows * 16,
            rows * 16,
            rows * 2 * 16 * 8 / 7,
            MIN_ENCODED_INDEX_SORTER_BYTES,
            rows * 16,
        ];
        let weights = encoded_sorter_budget_weights(Some(&estimates), estimates.len());
        let budgets = proportional_encoded_index_budgets(&weights, estimates.iter().sum());
        // Shares are floor-rounded, so each may fall short by under one byte
        // per sorter; real estimates carry row-count slack beyond that.
        for (budget, estimate) in budgets.iter().zip(estimates) {
            assert!(
                budget + estimates.len() >= estimate,
                "budget {budget} < estimate {estimate}"
            );
        }
        assert_eq!(encoded_sorter_budget_weights(None, 3), [1, 1, 1]);
    }

    #[test]
    fn fixed_entry_prefetch_preserves_values_before_error_and_early_consumer_exit() {
        let source = (0..8209u64).map(Ok).chain(std::iter::once(Err(error(
            crate::error::catalog::ER_DUP_ENTRY,
            "terminal error",
        ))));
        build_with_prefetched_fixed_entries(source, true, |mut stream| {
            assert!(matches!(&stream, PrefetchedFixedEntries::Buffered { .. }));
            for expected in 0..8209u64 {
                assert_eq!(stream.next().unwrap().unwrap(), expected);
            }
            let err = stream.next().unwrap().unwrap_err();
            assert!(err.is(crate::error::catalog::ER_DUP_ENTRY));
            assert!(stream.next().is_none());
            Ok(())
        })
        .unwrap();

        let stopped: Result<(), QueryError> =
            build_with_prefetched_fixed_entries((0..100_000u64).map(Ok), true, |mut stream| {
                assert_eq!(stream.next().unwrap().unwrap(), 0);
                Err(error(
                    crate::error::catalog::ER_QUERY_INTERRUPTED,
                    "consumer stopped",
                ))
            });
        assert!(stopped
            .unwrap_err()
            .is(crate::error::catalog::ER_QUERY_INTERRUPTED));
    }

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn compact_fixed128_sort_restores_sparse_bits_and_all_width_boundaries() {
        if !std::is_x86_feature_detected!("bmi2") {
            return;
        }
        for mask in [
            0,
            u128::from(u64::MAX),
            u128::from(u64::MAX) << 64,
            (u128::from(u32::MAX) << 80) | (0x00ff_ffffu128 << 24),
            u128::MAX,
        ] {
            let fixed = 0xa55a_9812_3311_2345_8765_4321_abcdef01u128 & !mask;
            let mut seed = 71u128;
            let mut records = (0..70_003)
                .map(|_| {
                    seed = seed.wrapping_mul(0xda942042e4dd58b5).wrapping_add(1);
                    (seed & mask) | fixed
                })
                .collect::<Vec<_>>();
            let original = records.clone();
            let mut expected = records.clone();
            expected.sort_unstable();
            let capacity = records.capacity();
            // SAFETY: the runtime feature check above covers every invocation.
            let packed = unsafe { sort_compact_fixed128_bmi2(&mut records) };
            if packed {
                assert_eq!(records, expected, "mask={mask:x}");
            } else {
                assert_eq!(records, original, "ineligible input must remain untouched");
            }
            assert_eq!(packed, mask.count_ones() <= 64);
            assert_eq!(records.capacity(), capacity);
        }
    }

    #[test]
    fn fixed_batch_admission_matches_scalar_across_spill_boundaries() {
        let root = tempfile::tempdir().unwrap();
        for arity in [1, 3] {
            let (program, _) = record_for(
                vec![ParamTarget::Int; arity],
                IndexKey::from(vec![KeyPart::Int(0); arity]),
                RowId {
                    page_id: 0,
                    slot_id: 0,
                },
            );
            let program = Arc::new(program);
            let mut scalar = EncodedIndexSorter::new(program.clone(), 1024 * 1024);
            let mut batched = EncodedIndexSorter::new(program.clone(), 1024 * 1024);
            let capacity = batched.capacity_bytes();
            let mut bytes = Vec::new();
            for value in (0..75_000u32).rev() {
                let key = IndexKey::from(
                    (0..arity)
                        .map(|column| {
                            if value % 17 == 0 {
                                KeyPart::Null
                            } else {
                                KeyPart::Int((value as i64 * 31 + column as i64) % 1009 - 500)
                            }
                        })
                        .collect::<Vec<_>>(),
                );
                let pending = program.encode_key(&key).unwrap();
                let row_id = RowId {
                    page_id: value / 2 + 1,
                    slot_id: (value % 13) as u16,
                };
                bytes.extend_from_slice(&pending.order_key);
                bytes.extend_from_slice(&row_id.page_id.to_be_bytes());
                bytes.extend_from_slice(&row_id.slot_id.to_be_bytes());
                scalar.push(root.path(), pending, row_id).unwrap();
                if value % 4097 == 0 {
                    batched.push_fixed_batch(root.path(), &bytes).unwrap();
                    assert_eq!(batched.capacity_bytes(), capacity);
                    bytes.clear();
                }
            }
            assert!(!batched.runs.is_empty());
            let expected = scalar
                .finish(root.path())
                .unwrap()
                .into_record_iter(program.clone())
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let actual = batched
                .finish(root.path())
                .unwrap()
                .into_record_iter(program)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(actual, expected, "arity={arity}");
        }
    }

    #[test]
    fn fixed128_sorter_spill_preserves_numeric_order_and_row_ids() {
        let root = tempfile::tempdir().unwrap();
        let (program, _) = record_for(
            vec![ParamTarget::Int],
            IndexKey::from(vec![KeyPart::Int(0)]),
            RowId {
                page_id: 0,
                slot_id: 0,
            },
        );
        let program = Arc::new(program);
        let mut sorter = EncodedIndexSorter::new(program.clone(), 1024 * 1024);
        for value in (0..100_000u32).rev() {
            sorter
                .push(
                    root.path(),
                    program
                        .encode_key(&IndexKey::from(vec![KeyPart::Int((value % 1000) as i64)]))
                        .unwrap(),
                    RowId {
                        page_id: value + 1,
                        slot_id: 0,
                    },
                )
                .unwrap();
        }
        let sorted = sorter.finish(root.path()).unwrap();
        let EncodedSortedRecords::Spilled { runs, fixed128, .. } = &sorted else {
            panic!("fixed sorter should spill with a 1 MiB envelope");
        };
        assert_eq!(*fixed128, Some((9, 15)));
        assert!(runs.len() > 1);
        assert!(runs
            .iter()
            .all(|path| fs::metadata(path).unwrap().len() % 16 == 0));
        let source = validated_fixed128_from_sorted(sorted, program, &dummy_index()).unwrap();
        let mut previous = None;
        let mut count = 0;
        for record in source {
            let record = record.unwrap();
            let value = decode_fixed_signed(&record[..9], 8);
            let row_id = decode_fixed128_row_id(&record, 9);
            if let Some((last_value, last_row_id)) = previous {
                assert!((last_value, last_row_id) <= (value, row_id));
            }
            previous = Some((value, row_id));
            count += 1;
        }
        assert_eq!(count, 100_000);
    }

    #[test]
    fn fixed128_spilled_unique_allows_nulls_and_rejects_cross_run_duplicate() {
        let root = tempfile::tempdir().unwrap();
        let tmp = IndexSortTmpDir::create(root.path()).unwrap();
        let (program, _) = record_for(
            vec![ParamTarget::Int],
            IndexKey::from(vec![KeyPart::Int(0)]),
            RowId {
                page_id: 0,
                slot_id: 0,
            },
        );
        let encode = |key: KeyPart, page_id: u32| {
            let order = program
                .encode_key(&IndexKey::from(vec![key]))
                .unwrap()
                .order_key;
            let mut record = [0u8; 16];
            record[..9].copy_from_slice(&order);
            record[9..13].copy_from_slice(&page_id.to_be_bytes());
            u128::from_be_bytes(record)
        };
        let mut runs = Vec::new();
        for run in 0..2u32 {
            let path = tmp.path.join(format!("unique-{run}"));
            let mut writer = EncodedFixed128RunCodec::create_writer(&path).unwrap();
            for record in [
                encode(KeyPart::Null, run + 1),
                encode(KeyPart::Int(7), run + 3),
            ] {
                EncodedFixed128RunCodec::append(&mut writer, &record).unwrap();
            }
            EncodedFixed128RunCodec::finish(writer).unwrap();
            runs.push(path);
        }
        let mut index = dummy_index();
        index.unique = true;
        let sorted = EncodedSortedRecords::Spilled {
            tmp,
            runs,
            fixed128: Some((9, 15)),
        };
        let mut source = validated_fixed128_from_sorted(sorted, Arc::new(program), &index).unwrap();
        assert!(source.next().unwrap().is_ok());
        assert!(source.next().unwrap().is_ok());
        assert!(source.next().unwrap().is_ok());
        assert!(source.next().unwrap().is_err());
    }

    #[test]
    fn overlapping_buffer_leases_restore_only_after_the_last_owner() {
        let mut state = OfflineIndexBuildBufferLeaseState::default();
        let (older, effective) = state.add(76_800, 40_000);
        assert_eq!(effective, 40_000);
        let (newer, effective) = state.add(40_000, 1_024);
        assert_eq!(effective, 1_024);
        assert_eq!(state.effective_for_configured_capacity(90_000), 1_024);
        assert_eq!(state.effective_for_configured_capacity(512), 512);

        // CREATE INDEX statements on different tables can complete in this
        // order. Releasing the older owner must not restore its 76,800-page
        // capacity underneath the newer owner.
        assert_eq!(state.remove(older), Some(1_024));
        assert_eq!(state.remove(newer), Some(76_800));
        assert!(state.active_limits.is_empty());
        assert_eq!(state.restore_capacity_pages, None);
    }

    #[test]
    fn governed_headroom_is_reclaimed_below_the_rss_watermark() {
        const OPERATIONAL_LIMIT: usize = 1_677_721_600;
        const OBSERVED_USED: usize = 1_668_257_471;
        const MIN_SORTER: usize = 16 * 1024 * 1024;
        assert_eq!(
            governed_index_build_reclaim_bytes_for_values(
                OBSERVED_USED,
                MIN_SORTER,
                OPERATIONAL_LIMIT,
            ),
            7_313_087
        );
        assert_eq!(
            governed_index_build_reclaim_bytes_for_values(
                OPERATIONAL_LIMIT - MIN_SORTER,
                MIN_SORTER,
                OPERATIONAL_LIMIT,
            ),
            0
        );
        assert_eq!(
            index_build_ancillary_headroom_for_limit(OPERATIONAL_LIMIT),
            64 * 1024 * 1024
        );
        assert_eq!(
            index_build_ancillary_headroom_for_limit(100 * 1024 * 1024),
            8 * 1024 * 1024
        );
        assert_eq!(
            index_build_ancillary_headroom_for_limit(16 * 1024 * 1024),
            4 * 1024 * 1024
        );
    }

    fn dummy_index() -> IndexSchema {
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "encoded_test".into(),
            columns: Vec::new(),
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: false,
            primary: false,
            included_columns: Vec::new(),
            state: IndexState::Public,
            index_type: "BTREE".into(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        }
    }

    fn record_for(
        schema: Vec<ParamTarget>,
        key: IndexKey,
        row_id: RowId,
    ) -> (CompiledIndexKeyProgram, EncodedIndexRecord) {
        let fixed_ops = schema
            .iter()
            .map(|target| match target {
                ParamTarget::Int => Some(FixedKeyOp::Signed(8)),
                ParamTarget::UInt => Some(FixedKeyOp::Unsigned(8)),
                ParamTarget::Float32 => Some(FixedKeyOp::Float32),
                ParamTarget::Float64 => Some(FixedKeyOp::Float64),
                ParamTarget::Decimal { precision, scale } if *precision <= 38 => {
                    Some(FixedKeyOp::DecimalI128 { scale: *scale })
                }
                ParamTarget::Decimal { scale, .. } => {
                    Some(FixedKeyOp::DecimalWide { scale: *scale })
                }
                ParamTarget::Date => Some(FixedKeyOp::Date),
                ParamTarget::Time => Some(FixedKeyOp::Time),
                ParamTarget::DateTime | ParamTarget::Timestamp => Some(FixedKeyOp::DateTime),
                ParamTarget::Year => Some(FixedKeyOp::Year),
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        let program = CompiledIndexKeyProgram {
            index: dummy_index(),
            direct_parts: None,
            decode_schema: schema,
            fixed_signed_widths: fixed_ops.as_ref().and_then(|ops| {
                ops.iter()
                    .map(|op| match op {
                        FixedKeyOp::Signed(width) => Some(*width),
                        _ => None,
                    })
                    .collect()
            }),
            fixed_ops,
            physical_lane: EncodedPhysicalLane::Generic,
        };
        let pending = program.encode_key(&key).unwrap();
        let record = program.attach_row_id(pending, row_id);
        (program, record)
    }

    #[test]
    fn fixed_null_padding_preserves_index_key_order_for_arbitrary_arity() {
        let schema = vec![ParamTarget::Int, ParamTarget::UInt, ParamTarget::Date];
        let keys = vec![
            IndexKey::from(vec![KeyPart::Null, KeyPart::UInt(7), KeyPart::Date(1)]),
            IndexKey::from(vec![
                KeyPart::Int(i64::MIN),
                KeyPart::Null,
                KeyPart::Date(-1),
            ]),
            IndexKey::from(vec![KeyPart::Int(-1), KeyPart::UInt(0), KeyPart::Null]),
            IndexKey::from(vec![
                KeyPart::Int(0),
                KeyPart::UInt(u64::MAX),
                KeyPart::Date(0),
            ]),
            IndexKey::from(vec![
                KeyPart::Int(i64::MAX),
                KeyPart::UInt(u64::MAX),
                KeyPart::Date(i32::MAX),
            ]),
        ];
        for left in &keys {
            for right in &keys {
                let (_, left_record) = record_for(
                    schema.clone(),
                    left.clone(),
                    RowId {
                        page_id: 1,
                        slot_id: 0,
                    },
                );
                let (program, right_record) = record_for(
                    schema.clone(),
                    right.clone(),
                    RowId {
                        page_id: 2,
                        slot_id: 0,
                    },
                );
                assert_eq!(
                    left.cmp(right),
                    left_record.order_key.cmp(&right_record.order_key)
                );
                assert_eq!(program.materialize_key(&right_record).unwrap(), *right);
            }
        }
    }

    #[test]
    fn variable_and_collated_keys_round_trip_without_losing_original_text() {
        let collation_id = crate::charset::collation_id("utf8mb4_0900_ai_ci");
        let schema = vec![
            ParamTarget::String,
            ParamTarget::CollatedString { collation_id },
            ParamTarget::Bytes,
            ParamTarget::Decimal {
                precision: 65,
                scale: 4,
            },
        ];
        let key = IndexKey::from(vec![
            KeyPart::String("a\0z".into()),
            KeyPart::CollatedString {
                collation_id,
                value: "Äbc".into(),
            },
            KeyPart::Bytes(vec![0, 1, 255]),
            KeyPart::DecimalWide {
                value: num_bigint::BigInt::parse_bytes(
                    b"123456789012345678901234567890123456789",
                    10,
                )
                .unwrap(),
                scale: 4,
            },
        ]);
        let (program, record) = record_for(
            schema,
            key.clone(),
            RowId {
                page_id: 9,
                slot_id: 3,
            },
        );
        assert!(record.materialized_key.is_some());
        assert_eq!(program.materialize_key(&record).unwrap(), key);
        assert_eq!(program.into_index_entry(record).unwrap().row_id.page_id, 9);
    }

    #[test]
    fn unified_fixed_sorter_orders_arbitrary_integer_arity_without_type_variants() {
        let schema = vec![ParamTarget::Int, ParamTarget::Int, ParamTarget::UInt];
        let (program, _) = record_for(
            schema,
            IndexKey::from(vec![KeyPart::Int(0), KeyPart::Int(0), KeyPart::UInt(0)]),
            RowId {
                page_id: 0,
                slot_id: 0,
            },
        );
        let program = Arc::new(program);
        let root = tempfile::tempdir().unwrap();
        let mut sorter = EncodedIndexSorter::new(program.clone(), 1024 * 1024);
        // Cross the original Vec capacities. Before the capacity-bound spill
        // fix the source byte/ordinal Vecs grew geometrically here and the
        // radix scratch made the actual peak exceed the 1 MiB reservation.
        for value in (0..40_000i64).rev() {
            let key = IndexKey::from(vec![
                KeyPart::Int(value % 97 - 48),
                KeyPart::Int(value),
                KeyPart::UInt(value as u64),
            ]);
            sorter
                .push(
                    root.path(),
                    program.encode_key(&key).unwrap(),
                    RowId {
                        page_id: value as u32,
                        slot_id: 0,
                    },
                )
                .unwrap();
        }
        assert!(sorter.capacity_bytes() <= 1024 * 1024);
        let mut previous = None;
        let mut count = 0;
        for record in sorter
            .finish(root.path())
            .unwrap()
            .into_record_iter(program.clone())
            .unwrap()
        {
            let record = record.unwrap();
            let key = program.materialize_key(&record).unwrap();
            if let Some(previous) = &previous {
                assert!(previous <= &key);
            }
            previous = Some(key);
            count += 1;
        }
        assert_eq!(count, 40_000);
    }

    #[test]
    fn wide_fixed_sorter_parallel_indirect_path_preserves_total_record_order() {
        let schema = vec![ParamTarget::Int, ParamTarget::Date, ParamTarget::Timestamp];
        let (program, _) = record_for(
            schema,
            IndexKey::from(vec![
                KeyPart::Int(0),
                KeyPart::Date(0),
                KeyPart::DateTime(0),
            ]),
            RowId {
                page_id: 0,
                slot_id: 0,
            },
        );
        let program = Arc::new(program);
        let root = tempfile::tempdir().unwrap();
        let rows = 256 * 1024 + 257;
        let mut sorter =
            EncodedIndexSorter::new(program.clone(), 10 * 1024 * 1024).with_sort_degree(4);
        for value in (0..rows as i64).rev() {
            let key = IndexKey::from(vec![
                KeyPart::Int(value % 10_007),
                KeyPart::Date((value % 20_000) as i32),
                KeyPart::DateTime(value * 1_000_000),
            ]);
            sorter
                .push(
                    root.path(),
                    program.encode_key(&key).unwrap(),
                    RowId {
                        page_id: value as u32,
                        slot_id: 0,
                    },
                )
                .unwrap();
        }
        let EncodedSortedRecords::Fixed {
            key_width,
            stride,
            bytes,
            order,
        } = sorter.finish(root.path()).unwrap()
        else {
            panic!("expected resident wide fixed records");
        };
        assert_eq!(order.len(), rows);
        let mut previous: Option<&[u8]> = None;
        for ordinal in order {
            let start = ordinal as usize * stride;
            let record = &bytes[start..start + stride];
            if let Some(previous) = previous {
                assert!(previous <= record);
            }
            previous = Some(record);
        }
        assert_eq!(key_width + 6, stride);
    }

    #[test]
    fn unified_variable_sorter_spills_merges_and_preserves_collated_payload() {
        let collation_id = crate::charset::collation_id("utf8mb4_0900_ai_ci");
        let schema = vec![ParamTarget::CollatedString { collation_id }];
        let (program, _) = record_for(
            schema,
            IndexKey::from(vec![KeyPart::CollatedString {
                collation_id,
                value: "seed".into(),
            }]),
            RowId {
                page_id: 0,
                slot_id: 0,
            },
        );
        let program = Arc::new(program);
        let root = tempfile::tempdir().unwrap();
        let mut sorter = EncodedIndexSorter::new(program.clone(), 1024 * 1024);
        for value in (0..12_000u32).rev() {
            let original = format!("Value-{value:08}-{}", "x".repeat(64));
            let key = IndexKey::from(vec![KeyPart::CollatedString {
                collation_id,
                value: original,
            }]);
            sorter
                .push(
                    root.path(),
                    program.encode_key(&key).unwrap(),
                    RowId {
                        page_id: value,
                        slot_id: 0,
                    },
                )
                .unwrap();
        }
        let sorted = sorter.finish(root.path()).unwrap();
        assert!(matches!(sorted, EncodedSortedRecords::Spilled { .. }));
        let mut previous = None;
        let mut count = 0;
        for record in sorted.into_record_iter(program.clone()).unwrap() {
            let record = record.unwrap();
            assert!(record.materialized_key.is_some());
            let key = program.materialize_key(&record).unwrap();
            if let Some(previous) = &previous {
                assert!(previous <= &key);
            }
            previous = Some(key);
            count += 1;
        }
        assert_eq!(count, 12_000);
    }

    #[test]
    fn low_cardinality_variable_sorter_interns_and_preserves_original_payloads() {
        let collation_id = crate::charset::collation_id("utf8mb4_0900_ai_ci");
        let schema = vec![ParamTarget::CollatedString { collation_id }];
        let (program, _) = record_for(
            schema,
            IndexKey::from(vec![KeyPart::CollatedString {
                collation_id,
                value: "seed".into(),
            }]),
            RowId {
                page_id: 0,
                slot_id: 0,
            },
        );
        let program = Arc::new(program);
        let root = tempfile::tempdir().unwrap();
        let mut sorter =
            EncodedIndexSorter::new(program.clone(), 16 * 1024 * 1024).with_sort_degree(4);
        let originals = ["Alpha", "ALPHA", "beta", "BETA"];
        for value in (0..70_000u32).rev() {
            let original = originals[value as usize % originals.len()];
            let key = IndexKey::from(vec![KeyPart::CollatedString {
                collation_id,
                value: original.into(),
            }]);
            sorter
                .push(
                    root.path(),
                    program.encode_key(&key).unwrap(),
                    RowId {
                        page_id: value,
                        slot_id: 0,
                    },
                )
                .unwrap();
        }
        let sorted = sorter.finish(root.path()).unwrap();
        assert!(matches!(&sorted, EncodedSortedRecords::Interned { keys, .. } if keys.len() == 4));
        if let EncodedSortedRecords::Interned { keys, records } = &sorted {
            let image = encode_interned_leaf_page(keys, &records[..100], None).unwrap();
            let IndexPage::Leaf(page) = decode_index_tree_page(&image).unwrap() else {
                panic!("expected interned leaf page");
            };
            assert_eq!(page.entries.len(), 100);
            for entry in page.entries {
                let KeyPart::CollatedString { value, .. } = &entry.key.0[0] else {
                    panic!("expected collated string page key");
                };
                assert_eq!(
                    value,
                    originals[entry.row_id.page_id as usize % originals.len()]
                );
            }
        }
        let mut previous = None;
        let mut count = 0usize;
        for record in sorted.into_record_iter(program.clone()).unwrap() {
            let record = record.unwrap();
            let row_id = record.row_id;
            let key = program.materialize_key(&record).unwrap();
            let KeyPart::CollatedString { value, .. } = &key.0[0] else {
                panic!("expected collated string");
            };
            assert_eq!(value, originals[row_id.page_id as usize % originals.len()]);
            if let Some(previous) = &previous {
                assert!(previous <= &key);
            }
            previous = Some(key);
            count += 1;
        }
        assert_eq!(count, 70_000);
    }
}
