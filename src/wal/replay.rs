//! Validated WAL recovery snapshots. Retain immutable segment inodes and a
//! compact, governed directory; do not rewrite or repeatedly read every frame.
use super::*;
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use std::borrow::Cow;

pub(crate) struct RecordLease<'a> {
    record: Cow<'a, WalRecord>,
    _charge: Option<DecodeReservation>,
}
impl std::ops::Deref for RecordLease<'_> {
    type Target = WalRecord;
    fn deref(&self) -> &WalRecord {
        &self.record
    }
}
impl AsRef<WalRecord> for RecordLease<'_> {
    fn as_ref(&self) -> &WalRecord {
        &self.record
    }
}
#[derive(Debug)]
struct DecodeBudget {
    used: std::sync::atomic::AtomicUsize,
    limit: usize,
    _charge: ControlledMemoryCharge,
}
enum DecodeReservation {
    Direct {
        _charge: ControlledMemoryCharge,
    },
    Pooled {
        budget: Arc<DecodeBudget>,
        bytes: usize,
    },
}
impl Drop for DecodeReservation {
    fn drop(&mut self) {
        if let Self::Pooled { budget, bytes } = self {
            budget.used.fetch_sub(*bytes, Ordering::Relaxed);
        }
    }
}
thread_local! {
    static DECODE_BUDGET_CACHE: std::cell::RefCell<(u64, Option<std::sync::Weak<DecodeBudget>>)> = const { std::cell::RefCell::new((0, None)) };
}
static NEXT_REPLAY_BUDGET_ID: AtomicU64 = AtomicU64::new(1);

pub(super) fn replay_decode_charge(bytes: usize) -> Result<ControlledMemoryCharge, QueryError> {
    ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::StorageScratch,
        bytes.saturating_mul(32).saturating_add(4096),
    )
}

pub(crate) struct UndoWalRecord<'a> {
    pub undo_id: u64,
    pub bytes: Cow<'a, [u8]>,
}

pub(crate) trait WalRecordSource {
    fn len(&self) -> usize;
    fn record(&self, index: usize) -> Result<RecordLease<'_>, QueryError>;
    fn header(&self, index: usize) -> Result<WalRecordHeader, QueryError> {
        let record = self.record(index)?;
        Ok(WalRecordHeader {
            source_time_us: record.source_time_us,
            lsn: record.lsn,
            prev_txn_lsn: record.prev_txn_lsn,
            txn_id: record.txn_id,
            record_type: record.payload.record_type(),
        })
    }
    fn redo_path(&self, index: usize) -> Result<Option<Cow<'_, str>>, QueryError> {
        Ok(payload_redo_path(&self.record(index)?.payload).map(|s| Cow::Owned(s.to_owned())))
    }
    fn undo_record(&self, index: usize) -> Result<Option<UndoWalRecord<'_>>, QueryError> {
        if self.header(index)?.record_type != WalRecordType::UndoRecord {
            return Ok(None);
        }
        let record = self.record(index)?;
        let WalPayload::UndoRecord { undo_id, bytes } = &record.payload else {
            return Err(corrupt_directory());
        };
        Ok(Some(UndoWalRecord {
            undo_id: *undo_id,
            bytes: Cow::Owned(bytes.clone()),
        }))
    }
    fn zone_delta(&self, index: usize) -> Result<Option<Cow<'_, [u8]>>, QueryError> {
        if self.header(index)?.record_type != WalRecordType::ZoneMapMergeBatch {
            return Ok(None);
        }
        let record = self.record(index)?;
        Ok(match &record.payload {
            WalPayload::ZoneMapMergeBatch { bytes } => Some(Cow::Owned(bytes.clone())),
            _ => None,
        })
    }
    fn index_operations(&self, index: usize) -> Result<usize, QueryError> {
        if self.header(index)?.record_type != WalRecordType::IndexBatch {
            return Ok(0);
        }
        let record = self.record(index)?;
        Ok(match &record.payload {
            WalPayload::IndexBatch { ops, .. } => ops.len(),
            _ => 0,
        })
    }
    fn bootstrap(&self) -> Option<&ReplayBootstrap> {
        None
    }
    fn records(&self) -> RecordIter<'_, Self> {
        RecordIter {
            source: self,
            front: 0,
            back: self.len(),
            types: &[],
        }
    }
}
impl WalRecordSource for [WalRecord] {
    fn len(&self) -> usize {
        <[WalRecord]>::len(self)
    }
    fn record(&self, index: usize) -> Result<RecordLease<'_>, QueryError> {
        Ok(RecordLease {
            record: Cow::Borrowed(&self[index]),
            _charge: None,
        })
    }
}
impl WalRecordSource for Vec<WalRecord> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }
    fn record(&self, index: usize) -> Result<RecordLease<'_>, QueryError> {
        self.as_slice().record(index)
    }
}
impl<const N: usize> WalRecordSource for [WalRecord; N] {
    fn len(&self) -> usize {
        N
    }
    fn record(&self, index: usize) -> Result<RecordLease<'_>, QueryError> {
        self.as_slice().record(index)
    }
}
pub(crate) struct RecordIter<'a, S: ?Sized> {
    source: &'a S,
    front: usize,
    back: usize,
    types: &'static [WalRecordType],
}
impl<S: WalRecordSource + ?Sized> RecordIter<'_, S> {
    pub(crate) fn only(mut self, types: &'static [WalRecordType]) -> Self {
        self.types = types;
        self
    }
}
impl<'a, S: WalRecordSource + ?Sized> Iterator for RecordIter<'a, S> {
    type Item = Result<RecordLease<'a>, QueryError>;
    fn next(&mut self) -> Option<Self::Item> {
        while self.front < self.back {
            let index = self.front;
            self.front += 1;
            if !self.types.is_empty() {
                match self.source.header(index) {
                    Ok(h) if !self.types.contains(&h.record_type) => continue,
                    Err(e) => return Some(Err(e)),
                    _ => {}
                }
            }
            return Some(self.source.record(index));
        }
        None
    }
}
impl<S: WalRecordSource + ?Sized> DoubleEndedIterator for RecordIter<'_, S> {
    fn next_back(&mut self) -> Option<Self::Item> {
        while self.front < self.back {
            self.back -= 1;
            if !self.types.is_empty() {
                match self.source.header(self.back) {
                    Ok(h) if !self.types.contains(&h.record_type) => continue,
                    Err(e) => return Some(Err(e)),
                    _ => {}
                }
            }
            return Some(self.source.record(self.back));
        }
        None
    }
}

/// Existing valid bytes of a WAL segment are append-only. Checkpoint collapse
/// publishes a replacement inode and unlinks obsolete files; it never rewrites
/// the validated prefix. Pin the inode, and map only the validated prefix (not
/// the preallocated tail which opening the writer may truncate).
#[derive(Debug)]
struct ReplaySegment {
    _file: File,
    #[cfg(unix)]
    address: *mut libc::c_void,
    len: usize,
    #[cfg(unix)]
    resident: Vec<std::sync::atomic::AtomicBool>,
}
// SAFETY: the prefix is immutable and every reader uses its own byte range.
#[cfg(unix)]
unsafe impl Send for ReplaySegment {}
#[cfg(unix)]
unsafe impl Sync for ReplaySegment {}
impl ReplaySegment {
    fn open(file: File, len: u64) -> Result<Self, QueryError> {
        let len = usize::try_from(len)
            .ok()
            .filter(|n| *n <= isize::MAX as usize)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "WAL snapshot exceeds address space",
                )
            })?;
        #[cfg(unix)]
        let address = if len == 0 {
            std::ptr::null_mut()
        } else {
            // SAFETY: the file pins its inode; only the validated immutable
            // prefix is mapped. Recovery owns the database's exclusive lock.
            let p = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ,
                    libc::MAP_PRIVATE,
                    file.as_raw_fd(),
                    0,
                )
            };
            if p == libc::MAP_FAILED {
                return Err(io_err(std::io::Error::last_os_error()));
            }
            p
        };
        Ok(Self {
            _file: file,
            len,
            #[cfg(unix)]
            resident: (0..len.div_ceil(REPLAY_WINDOW))
                .map(|_| std::sync::atomic::AtomicBool::new(false))
                .collect(),
            #[cfg(unix)]
            address,
        })
    }
    fn bytes(&self, offset: u64, len: usize) -> Result<Cow<'_, [u8]>, QueryError> {
        let offset = usize::try_from(offset).map_err(|_| corrupt_directory())?;
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= self.len)
            .ok_or_else(corrupt_directory)?;
        #[cfg(unix)]
        {
            // SAFETY: bounds are checked against the pinned immutable prefix.
            Ok(Cow::Borrowed(unsafe {
                std::slice::from_raw_parts(self.address.cast::<u8>().add(offset), end - offset)
            }))
        }
        #[cfg(not(unix))]
        {
            let mut bytes = vec![0u8; end - offset];
            let mut done = 0;
            while done < bytes.len() {
                let n = match self
                    ._file
                    .read_at(&mut bytes[done..], (offset + done) as u64)
                {
                    Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                    other => other.map_err(io_err)?,
                };
                if n == 0 {
                    return Err(io_err(std::io::ErrorKind::UnexpectedEof.into()));
                }
                done += n;
            }
            Ok(Cow::Owned(bytes))
        }
    }
}
#[cfg(unix)]
impl Drop for ReplaySegment {
    fn drop(&mut self) {
        if self.len != 0 {
            // SAFETY: this is exactly the mapping owned by this object.
            unsafe {
                libc::munmap(self.address, self.len);
            }
        }
    }
}
fn corrupt_directory() -> QueryError {
    error(
        crate::error::catalog::ER_GET_ERRNO,
        "invalid recovery record location",
    )
}

#[derive(Debug, Clone, Copy)]
struct RecordLocation {
    source_time_us: u64,
    lsn: u64,
    prev_txn_lsn: u64,
    txn_id: u64,
    offset: u64,
    len: u32,
    segment_and_type: u32,
}
impl RecordLocation {
    #[inline]
    fn header(&self) -> Result<WalRecordHeader, QueryError> {
        Ok(WalRecordHeader {
            source_time_us: self.source_time_us,
            lsn: self.lsn,
            prev_txn_lsn: self.prev_txn_lsn,
            txn_id: self.txn_id,
            record_type: WalRecordType::from_u16((self.segment_and_type & 255) as u16)?,
        })
    }
}
#[cfg(unix)]
const REPLAY_WINDOW: usize = 64 * 1024 * 1024;
const DIRECTORY_CHUNK: usize = 4096;
#[derive(Debug, Default)]
struct RecordDirectory {
    chunks: Vec<Vec<RecordLocation>>,
    len: usize,
}
impl RecordDirectory {
    fn len(&self) -> usize {
        self.len
    }
    #[inline]
    fn get(&self, index: usize) -> Option<&RecordLocation> {
        self.chunks
            .get(index / DIRECTORY_CHUNK)?
            .get(index % DIRECTORY_CHUNK)
    }
    fn push(
        &mut self,
        record: RecordLocation,
        charge: &mut ControlledMemoryCharge,
    ) -> Result<(), QueryError> {
        if self.len % DIRECTORY_CHUNK == 0 {
            charge.try_grow(DIRECTORY_CHUNK * std::mem::size_of::<RecordLocation>())?;
            self.chunks.push(Vec::with_capacity(DIRECTORY_CHUNK));
        }
        self.chunks.last_mut().unwrap().push(record);
        self.len += 1;
        Ok(())
    }
    fn find(&self, lsn: u64) -> Option<usize> {
        let (mut low, mut high) = (0, self.len);
        while low < high {
            let mid = low + (high - low) / 2;
            match self.get(mid).unwrap().lsn.cmp(&lsn) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReplayBootstrap {
    pub max_source_time: u64,
    pub segments: Vec<WalSegmentInfo>,
    pub stats: WalStats,
    pub txn_bounds: Option<(u64, u64)>,
    pub txn_last_lsn: hashbrown::HashMap<u64, u64, ahash::RandomState>,
}
#[derive(Debug)]
pub(crate) struct ReplayScan {
    read_mode: WalReadMode,
    pub records: ReplayRecords,
    pub next_lsn: u64,
    pub end_offset: u64,
}
#[derive(Debug, Default)]
struct MappedWorkingSet {
    windows: std::collections::VecDeque<(usize, usize)>,
}

#[derive(Debug)]
pub(crate) struct ReplayRecords {
    segments: Vec<ReplaySegment>,
    locations: RecordDirectory,
    bootstrap: ReplayBootstrap,
    mapped: parking_lot::Mutex<MappedWorkingSet>,
    _directory_charge: ControlledMemoryCharge,
    budget_id: u64,
    budgets: parking_lot::Mutex<Vec<Arc<DecodeBudget>>>,
}
impl WalRecordSource for ReplayRecords {
    fn len(&self) -> usize {
        self.locations.len()
    }
    fn header(&self, index: usize) -> Result<WalRecordHeader, QueryError> {
        self.locations
            .get(index)
            .ok_or_else(corrupt_directory)?
            .header()
    }
    fn bootstrap(&self) -> Option<&ReplayBootstrap> {
        Some(&self.bootstrap)
    }
    fn record(&self, index: usize) -> Result<RecordLease<'_>, QueryError> {
        let loc = self.locations.get(index).ok_or_else(corrupt_directory)?;
        let header = loc.header()?;
        let charge = self.reserve_decode(loc.len as usize)?;
        let bytes = self.payload(loc)?;
        Ok(RecordLease {
            record: Cow::Owned(WalRecord {
                source_time_us: header.source_time_us,
                lsn: header.lsn,
                prev_txn_lsn: header.prev_txn_lsn,
                txn_id: header.txn_id,
                payload: decode_payload(header.record_type, &bytes)?,
            }),
            _charge: Some(charge),
        })
    }
    fn undo_record(&self, index: usize) -> Result<Option<UndoWalRecord<'_>>, QueryError> {
        let loc = self.locations.get(index).ok_or_else(corrupt_directory)?;
        if loc.header()?.record_type != WalRecordType::UndoRecord {
            return Ok(None);
        }
        let bytes = self.payload(loc)?;
        if bytes.len() < 12 {
            return Err(corrupt_directory());
        }
        let undo_id = read_u64(&bytes, 0);
        let len = read_u32(&bytes, 8) as usize;
        let bytes = match bytes {
            Cow::Borrowed(bytes) => {
                Cow::Borrowed(bytes.get(12..12 + len).ok_or_else(corrupt_directory)?)
            }
            Cow::Owned(bytes) => Cow::Owned(
                bytes
                    .get(12..12 + len)
                    .ok_or_else(corrupt_directory)?
                    .to_vec(),
            ),
        };
        Ok(Some(UndoWalRecord { undo_id, bytes }))
    }
    fn zone_delta(&self, index: usize) -> Result<Option<Cow<'_, [u8]>>, QueryError> {
        let loc = self.locations.get(index).ok_or_else(corrupt_directory)?;
        if loc.header()?.record_type != WalRecordType::ZoneMapMergeBatch {
            return Ok(None);
        }
        let bytes = self.payload(loc)?;
        let mut offset = 0;
        let len = read_u32_checked(&bytes, &mut offset)? as usize;
        Ok(Some(match bytes {
            Cow::Borrowed(bytes) => Cow::Borrowed(
                bytes
                    .get(offset..offset + len)
                    .ok_or_else(corrupt_directory)?,
            ),
            Cow::Owned(bytes) => Cow::Owned(
                bytes
                    .get(offset..offset + len)
                    .ok_or_else(corrupt_directory)?
                    .to_vec(),
            ),
        }))
    }
    fn index_operations(&self, index: usize) -> Result<usize, QueryError> {
        let loc = self.locations.get(index).ok_or_else(corrupt_directory)?;
        if loc.header()?.record_type != WalRecordType::IndexBatch {
            return Ok(0);
        }
        let bytes = self.payload(loc)?;
        let mut offset = 0;
        let path_len = read_u32_checked(&bytes, &mut offset)? as usize;
        offset = offset.checked_add(path_len).ok_or_else(corrupt_directory)?;
        Ok(read_u32_checked(&bytes, &mut offset)? as usize)
    }
    fn redo_path(&self, index: usize) -> Result<Option<Cow<'_, str>>, QueryError> {
        let loc = self.locations.get(index).ok_or_else(corrupt_directory)?;
        if !type_has_redo_path(loc.header()?.record_type) {
            return Ok(None);
        }
        let bytes = self.payload(loc)?;
        let len = read_u32(&bytes, 0) as usize;
        match bytes {
            Cow::Borrowed(bytes) => Ok(Some(Cow::Borrowed(
                std::str::from_utf8(bytes.get(4..4 + len).ok_or_else(corrupt_directory)?)
                    .map_err(|_| corrupt_directory())?,
            ))),
            Cow::Owned(bytes) => Ok(Some(Cow::Owned(
                std::str::from_utf8(bytes.get(4..4 + len).ok_or_else(corrupt_directory)?)
                    .map_err(|_| corrupt_directory())?
                    .to_owned(),
            ))),
        }
    }
}
impl ReplayRecords {
    fn reserve_decode(&self, bytes: usize) -> Result<DecodeReservation, QueryError> {
        let bytes = bytes.saturating_mul(32).saturating_add(4096);
        let budget = DECODE_BUDGET_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if cache.0 == self.budget_id {
                return cache.1.as_ref().and_then(std::sync::Weak::upgrade);
            }
            const LIMIT: usize = 4 * 1024 * 1024;
            let budget = ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::StorageScratch,
                LIMIT,
            )
            .map(|charge| {
                Arc::new(DecodeBudget {
                    used: std::sync::atomic::AtomicUsize::new(0),
                    limit: LIMIT,
                    _charge: charge,
                })
            });
            if let Some(budget) = &budget {
                self.budgets.lock().push(Arc::clone(budget));
            }
            *cache = (self.budget_id, budget.as_ref().map(Arc::downgrade));
            budget
        });
        if let Some(budget) = budget.filter(|b| bytes <= b.limit) {
            let previous = budget.used.fetch_add(bytes, Ordering::Relaxed);
            if previous <= budget.limit - bytes {
                return Ok(DecodeReservation::Pooled { budget, bytes });
            }
            budget.used.fetch_sub(bytes, Ordering::Relaxed);
        }
        Ok(DecodeReservation::Direct {
            _charge: ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::StorageScratch,
                bytes,
            )?,
        })
    }
    fn payload(&self, loc: &RecordLocation) -> Result<Cow<'_, [u8]>, QueryError> {
        let segment_index = (loc.segment_and_type >> 8) as usize;
        let segment = &self.segments[segment_index];
        loc.offset
            .checked_add(loc.len as u64)
            .filter(|end| *end <= segment.len as u64)
            .ok_or_else(corrupt_directory)?;
        #[cfg(unix)]
        if loc.len != 0 {
            let first = loc.offset as usize / REPLAY_WINDOW;
            let last = (loc.offset as usize + loc.len as usize - 1) / REPLAY_WINDOW;
            for window in first..=last {
                if segment.resident[window].load(Ordering::Relaxed) {
                    continue;
                }
                let mut mapped = self.mapped.lock();
                if segment.resident[window].load(Ordering::Relaxed) {
                    continue;
                }
                let capacity = (crate::engine::executor::mem_budget::memory_target_bytes() / 3)
                    .min(2 * 1024 * 1024 * 1024)
                    .max(REPLAY_WINDOW)
                    / REPLAY_WINDOW;
                while mapped.windows.len() >= capacity {
                    let (old_segment, old_window) = mapped.windows.pop_front().unwrap();
                    let old = &self.segments[old_segment];
                    let offset = old_window * REPLAY_WINDOW;
                    let len = REPLAY_WINDOW.min(old.len - offset);
                    // SAFETY: this page-aligned subrange belongs to an
                    // immutable file mapping. DONTNEED drops PTEs, not bytes;
                    // concurrent borrowed readers may safely fault them back.
                    let result = unsafe {
                        libc::madvise(
                            old.address.cast::<u8>().add(offset).cast(),
                            len,
                            libc::MADV_DONTNEED,
                        )
                    };
                    if result != 0 {
                        return Err(io_err(std::io::Error::last_os_error()));
                    }
                    old.resident[old_window].store(false, Ordering::Relaxed);
                }
                segment.resident[window].store(true, Ordering::Relaxed);
                mapped.windows.push_back((segment_index, window));
            }
        }
        segment.bytes(loc.offset, loc.len as usize)
    }
    pub fn find_lsn(&self, lsn: u64) -> Result<Option<RecordLease<'_>>, QueryError> {
        match self.locations.find(lsn) {
            Some(i) => self.record(i).map(Some),
            None => Ok(None),
        }
    }
}
fn type_has_redo_path(ty: WalRecordType) -> bool {
    matches!(
        ty,
        WalRecordType::FileClr
            | WalRecordType::FileCreate
            | WalRecordType::FileDelete
            | WalRecordType::FileExtend
            | WalRecordType::PageUpdate
            | WalRecordType::PageDelta
            | WalRecordType::PageUpdateCompressed
            | WalRecordType::FullPageImage
            | WalRecordType::FullPageImageCompressed
            | WalRecordType::HeapInsert
            | WalRecordType::HeapInsertBatch
            | WalRecordType::HeapUpdate
            | WalRecordType::HeapUpdateLogical
            | WalRecordType::HeapDelete
            | WalRecordType::SlotReclaim
            | WalRecordType::IndexBatch
            | WalRecordType::Clr
    )
}
fn payload_redo_path(payload: &WalPayload) -> Option<&str> {
    match payload {
        WalPayload::FileClr { path, .. }
        | WalPayload::FileCreate { path, .. }
        | WalPayload::FileDelete { path, .. }
        | WalPayload::FileExtend { path, .. }
        | WalPayload::PageUpdate { path, .. }
        | WalPayload::PageDelta { path, .. }
        | WalPayload::PageUpdateCompressed { path, .. }
        | WalPayload::FullPageImage { path, .. }
        | WalPayload::FullPageImageCompressed { path, .. }
        | WalPayload::HeapInsert { path, .. }
        | WalPayload::HeapInsertBatch { path, .. }
        | WalPayload::HeapUpdate { path, .. }
        | WalPayload::HeapUpdateLogical { path, .. }
        | WalPayload::HeapDelete { path, .. }
        | WalPayload::SlotReclaim { path, .. }
        | WalPayload::IndexBatch { path, .. }
        | WalPayload::Clr { path, .. } => Some(path),
        _ => None,
    }
}
struct SegmentReplayScan {
    max_source_time: u64,
    index: usize,
    mapping: ReplaySegment,
    info: WalSegmentInfo,
    locations: RecordDirectory,
    stats: WalStats,
    // Zero is a terminal-record tombstone. It must remove an earlier segment's
    // active txn tail when per-segment scans are combined in LSN order.
    txn_last_lsn: hashbrown::HashMap<u64, u64, ahash::RandomState>,
    next_lsn: u64,
    end: u64,
    _charge: ControlledMemoryCharge,
}
fn scan_replay_segment(
    path: &Path,
    id: u64,
    index: usize,
    truncate_tail: bool,
    mode: WalReadMode,
) -> Result<SegmentReplayScan, QueryError> {
    let file = File::open(path).map_err(io_err)?;
    let mut charge = replay_decode_charge(0)?;
    let mut locations = RecordDirectory::default();
    let mut stats = WalStats::default();
    let mut txn_last_lsn = hashbrown::HashMap::<u64, u64, ahash::RandomState>::default();
    let mut max_source_time = 0;
    let mut last_lsn = 0;
    let (next_lsn, end) = visit_segment_records_located_with_mode(
        path,
        id,
        truncate_tail,
        mode,
        |record, offset, len| {
            if record.lsn <= last_lsn {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt WAL: non-increasing recovery LSN",
                ));
            }
            last_lsn = record.lsn;
            max_source_time = max_source_time.max(record.source_time_us);
            locations.push(
                RecordLocation {
                    source_time_us: record.source_time_us,
                    lsn: record.lsn,
                    prev_txn_lsn: record.prev_txn_lsn,
                    txn_id: record.txn_id,
                    offset,
                    len: len as u32,
                    segment_and_type: ((index as u32) << 8) | record.payload.record_type() as u32,
                },
                &mut charge,
            )?;
            accumulate_wal_stats(&mut stats, &record.payload);
            if record.txn_id != 0 {
                if !txn_last_lsn.contains_key(&record.txn_id) {
                    charge.try_grow(64)?;
                }
                txn_last_lsn.insert(
                    record.txn_id,
                    if LogManager::is_terminal_txn_record(&record.payload) {
                        0
                    } else {
                        record.lsn
                    },
                );
            }
            Ok(())
        },
    )?;
    let first_lsn = locations.get(0).map_or(next_lsn, |loc| loc.lsn);
    let info = WalSegmentInfo {
        segment_id: id,
        path: path.to_owned(),
        first_lsn,
        last_lsn: next_lsn.saturating_sub(1),
        size_bytes: file.metadata().map_err(io_err)?.len(),
        status: WalSegmentStatus::Reclaimable,
    };
    Ok(SegmentReplayScan {
        max_source_time,
        index,
        mapping: ReplaySegment::open(file, end)?,
        info,
        locations,
        stats,
        txn_last_lsn,
        next_lsn,
        end,
        _charge: charge,
    })
}
fn merge_replay_stats(total: &mut WalStats, part: &WalStats) {
    macro_rules! add { ($($field:ident),*) => { $(total.$field = total.$field.saturating_add(part.$field);)* }; }
    add!(
        logical_records,
        physical_write_calls,
        physical_write_bytes,
        page_fallback_records,
        index_batch_records,
        heap_payload_bytes,
        index_payload_bytes,
        page_payload_bytes,
        heap_fpi_payload_bytes,
        index_page_payload_bytes,
        other_page_payload_bytes,
        sync_total_us,
        rotation_count,
        rotation_critical_total_us
    );
    total.sync_max_us = total.sync_max_us.max(part.sync_max_us);
    total.rotation_critical_max_us = total
        .rotation_critical_max_us
        .max(part.rotation_critical_max_us);
}

impl ReplayScan {
    /// Recovery appends CLRs/EndTxn to the immutable prefix. Validate only
    /// that new tail instead of decoding the complete retained WAL again.
    pub(crate) fn refresh(&mut self, data_dir: &Path) -> Result<(), QueryError> {
        let discovered = discover_segments(&crate::storage::naming::system_wal_dir(data_dir))?;
        let old_len = self.records.segments.len();
        if old_len == 0 {
            *self = LogManager::scan_replay_with_mode(data_dir, self.read_mode)?;
            return Ok(());
        }
        if discovered.len() > 0x0100_0000
            || discovered.len() < old_len
            || discovered
                .iter()
                .zip(&self.records.bootstrap.segments)
                .any(|((id, _), info)| *id != info.segment_id)
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "WAL generations changed during recovery",
            ));
        }
        let mut last_lsn = self.next_lsn.saturating_sub(1);
        for index in old_len - 1..discovered.len() {
            let (id, path) = &discovered[index];
            let file = File::open(path).map_err(io_err)?;
            let start = if index == old_len - 1 {
                self.end_offset
            } else {
                WAL_HEADER_LEN as u64
            };
            #[cfg(unix)]
            if index == old_len - 1 {
                use std::os::unix::fs::MetadataExt;
                let old = self.records.segments[index]
                    ._file
                    .metadata()
                    .map_err(io_err)?;
                let new = file.metadata().map_err(io_err)?;
                if old.ino() != new.ino() || old.dev() != new.dev() {
                    return Err(corrupt_directory());
                }
            }
            let first_index = self.records.len();
            let (next, end) = visit_segment_records_located_from(
                path,
                *id,
                index + 1 == discovered.len()
                    && !crate::storage::image_io::immutable_path(data_dir),
                self.read_mode,
                start,
                last_lsn,
                |record, offset, len| {
                    if record.lsn <= last_lsn {
                        return Err(corrupt_directory());
                    }
                    last_lsn = record.lsn;
                    self.records.bootstrap.max_source_time = self
                        .records
                        .bootstrap
                        .max_source_time
                        .max(record.source_time_us);
                    self.records.locations.push(
                        RecordLocation {
                            source_time_us: record.source_time_us,
                            lsn: record.lsn,
                            prev_txn_lsn: record.prev_txn_lsn,
                            txn_id: record.txn_id,
                            offset,
                            len: len as u32,
                            segment_and_type: ((index as u32) << 8)
                                | record.payload.record_type() as u32,
                        },
                        &mut self.records._directory_charge,
                    )?;
                    accumulate_wal_stats(&mut self.records.bootstrap.stats, &record.payload);
                    if record.txn_id != 0 {
                        self.records.bootstrap.txn_bounds =
                            Some(match self.records.bootstrap.txn_bounds {
                                Some((lo, hi)) => (lo.min(record.txn_id), hi.max(record.txn_id)),
                                None => (record.txn_id, record.txn_id),
                            });
                        if LogManager::is_terminal_txn_record(&record.payload) {
                            if self
                                .records
                                .bootstrap
                                .txn_last_lsn
                                .remove(&record.txn_id)
                                .is_some()
                            {
                                self.records._directory_charge.shrink(64);
                            }
                        } else {
                            if !self
                                .records
                                .bootstrap
                                .txn_last_lsn
                                .contains_key(&record.txn_id)
                            {
                                self.records._directory_charge.try_grow(64)?;
                            }
                            self.records
                                .bootstrap
                                .txn_last_lsn
                                .insert(record.txn_id, record.lsn);
                        }
                    }
                    Ok(())
                },
            )?;
            let first_lsn = if index < old_len {
                self.records.bootstrap.segments[index].first_lsn
            } else {
                self.records
                    .locations
                    .get(first_index)
                    .map_or(next, |loc| loc.lsn)
            };
            let mut info = WalSegmentInfo {
                segment_id: *id,
                path: path.clone(),
                first_lsn,
                last_lsn: next.saturating_sub(1),
                size_bytes: file.metadata().map_err(io_err)?.len(),
                status: if index + 1 == discovered.len() {
                    WalSegmentStatus::Active
                } else {
                    WalSegmentStatus::Reclaimable
                },
            };
            if start == WAL_HEADER_LEN as u64
                && self.records.len() == first_index
                && index + 1 != discovered.len()
            {
                info.first_lsn = FIRST_LSN;
                info.last_lsn = 0;
            }
            let mapping = ReplaySegment::open(file, end)?;
            if index < old_len {
                self.records
                    .mapped
                    .get_mut()
                    .windows
                    .retain(|(segment, _)| *segment != index);
                self.records.segments[index] = mapping;
                self.records.bootstrap.segments[index] = info;
            } else {
                self.records.segments.push(mapping);
                self.records.bootstrap.segments.push(info);
            }
            self.next_lsn = next;
            self.end_offset = end;
        }
        Ok(())
    }

    pub(crate) fn select(mut self, types: &[WalRecordType]) -> Result<ReplayRecords, QueryError> {
        let old = std::mem::take(&mut self.records.locations);
        for chunk in old.chunks {
            let capacity = chunk.capacity() * std::mem::size_of::<RecordLocation>();
            for loc in chunk {
                if types.contains(&loc.header()?.record_type) {
                    self.records
                        .locations
                        .push(loc, &mut self.records._directory_charge)?;
                }
            }
            self.records._directory_charge.shrink(capacity);
        }
        #[cfg(unix)]
        for segment in &self.records.segments {
            if segment.len != 0 {
                // SAFETY: discard resident pages of the still-pinned immutable
                // mapping. Selected payloads will fault in on demand later.
                if unsafe { libc::madvise(segment.address, segment.len, libc::MADV_DONTNEED) } != 0
                {
                    return Err(io_err(std::io::Error::last_os_error()));
                }
            }
            for flag in &segment.resident {
                flag.store(false, Ordering::Relaxed);
            }
        }
        self.records.mapped.get_mut().windows.clear();
        self.records.budgets.get_mut().clear();
        self.records.budget_id = NEXT_REPLAY_BUDGET_ID.fetch_add(1, Ordering::Relaxed);
        self.records.bootstrap = ReplayBootstrap::default();
        Ok(self.records)
    }
}

impl LogManager {
    pub(crate) fn scan_replay(data_dir: &Path) -> Result<ReplayScan, QueryError> {
        Self::scan_replay_with_mode(data_dir, WalReadMode::Recovery)
    }

    pub(crate) fn scan_replay_with_mode(
        data_dir: &Path,
        mode: WalReadMode,
    ) -> Result<ReplayScan, QueryError> {
        let discovered = discover_segments(&crate::storage::naming::system_wal_dir(data_dir))?;
        if discovered.len() > 0x0100_0000 {
            return Err(corrupt_directory());
        }
        let workers = std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .min(4)
            .min(discovered.len())
            .max(1);
        let jobs = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for worker in 0..workers {
                let discovered = &discovered;
                handles.push(scope.spawn(move || {
                    let mut results = Vec::new();
                    for index in (worker..discovered.len().saturating_sub(1)).step_by(workers) {
                        let (id, path) = &discovered[index];
                        results.push(scan_replay_segment(path, *id, index, false, mode)?);
                    }
                    Ok::<_, QueryError>(results)
                }));
            }
            let mut jobs = Vec::new();
            for handle in handles {
                jobs.extend(handle.join().map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "WAL scan worker panicked",
                    )
                })??);
            }
            Ok::<_, QueryError>(jobs)
        })?;
        let mut jobs = jobs;
        if let Some((id, path)) = discovered.last() {
            jobs.push(scan_replay_segment(
                path,
                *id,
                discovered.len() - 1,
                !crate::storage::image_io::immutable_path(data_dir),
                mode,
            )?);
        }
        jobs.sort_unstable_by_key(|job| job.index);
        let mut store = ReplayRecords {
            segments: Vec::new(),
            locations: RecordDirectory::default(),
            bootstrap: ReplayBootstrap::default(),
            mapped: parking_lot::Mutex::new(MappedWorkingSet::default()),
            _directory_charge: replay_decode_charge(0)?,
            budget_id: NEXT_REPLAY_BUDGET_ID.fetch_add(1, Ordering::Relaxed),
            budgets: parking_lot::Mutex::new(Vec::new()),
        };
        let mut last_lsn = 0;
        let mut next_lsn = FIRST_LSN;
        let mut end_offset = 0;
        for job in jobs {
            store.bootstrap.max_source_time =
                store.bootstrap.max_source_time.max(job.max_source_time);
            if let Some(first) = job.locations.get(0) {
                if first.lsn <= last_lsn {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt WAL: non-increasing recovery LSN",
                    ));
                }
                last_lsn = job.locations.get(job.locations.len() - 1).unwrap().lsn;
            }
            for chunk in job.locations.chunks {
                for loc in chunk {
                    if loc.txn_id != 0 {
                        store.bootstrap.txn_bounds = Some(match store.bootstrap.txn_bounds {
                            Some((lo, hi)) => (lo.min(loc.txn_id), hi.max(loc.txn_id)),
                            None => (loc.txn_id, loc.txn_id),
                        });
                    }
                    store.locations.push(loc, &mut store._directory_charge)?;
                }
            }
            merge_replay_stats(&mut store.bootstrap.stats, &job.stats);
            for (txn, lsn) in job.txn_last_lsn {
                if lsn == 0 {
                    if store.bootstrap.txn_last_lsn.remove(&txn).is_some() {
                        store._directory_charge.shrink(64);
                    }
                } else {
                    if !store.bootstrap.txn_last_lsn.contains_key(&txn) {
                        store._directory_charge.try_grow(64)?;
                    }
                    store.bootstrap.txn_last_lsn.insert(txn, lsn);
                }
            }
            next_lsn = next_lsn.max(job.next_lsn);
            end_offset = job.end;
            store.bootstrap.segments.push(job.info);
            store.segments.push(job.mapping);
        }
        if let Some(active) = store.bootstrap.segments.last_mut() {
            if active.last_lsn == 0 {
                active.first_lsn = next_lsn;
            }
            active.last_lsn = last_lsn;
            active.status = WalSegmentStatus::Active;
        }
        Ok(ReplayScan {
            read_mode: mode,
            records: store,
            next_lsn,
            end_offset,
        })
    }
    pub(crate) fn open_from_replay(data_dir: &Path, scan: &ReplayScan) -> Result<Self, QueryError> {
        Self::open_from_source(data_dir, &scan.records, scan.next_lsn, scan.end_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_snapshot_matches_scan_and_survives_writer_append() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        log.set_max_segment_bytes(2048);
        for txn in 1..=40 {
            log.append(txn, WalPayload::BeginTxn { start_scn: txn })
                .unwrap();
            log.append(
                txn,
                WalPayload::FileCreate {
                    path: format!("objects/{txn}.heap"),
                    // A payload crossing the small segment boundary exercises
                    // cross-segment ordering without retaining segment buffers.
                    after: vec![txn as u8; 512],
                },
            )
            .unwrap();
            log.append(
                txn,
                WalPayload::CommitTxn {
                    commit_scn: txn + 1,
                    commit_unix_us: 0,
                    binlog: None,
                },
            )
            .unwrap();
        }
        log.append_txn_batch(
            41,
            vec![
                WalPayload::UndoRecord {
                    undo_id: 9,
                    bytes: vec![1, 2, 3],
                },
                WalPayload::IndexBatch {
                    path: "objects/41.idx".into(),
                    ops: Vec::new(),
                },
                WalPayload::CommitTxn {
                    commit_scn: 42,
                    commit_unix_us: 0,
                    binlog: None,
                },
            ],
        )
        .unwrap();
        drop(log);
        let expected = LogManager::scan(dir.path()).unwrap();
        let replay = LogManager::scan_replay(dir.path()).unwrap();
        assert_eq!(replay.records.len(), expected.records.len());
        assert_eq!(replay.next_lsn, expected.next_lsn);
        assert_eq!(replay.end_offset, expected.end_offset);
        for (expected, actual) in expected.records.iter().zip(replay.records.records()) {
            assert_eq!(
                encode_record(expected).unwrap(),
                encode_record(&actual.unwrap()).unwrap()
            );
            let found = replay.records.find_lsn(expected.lsn).unwrap().unwrap();
            assert_eq!(
                encode_record(expected).unwrap(),
                encode_record(&found).unwrap()
            );
        }
        let expected_commits = expected
            .records
            .iter()
            .filter(|record| matches!(record.payload, WalPayload::CommitTxn { .. }))
            .map(|record| record.lsn)
            .collect::<Vec<_>>();
        let actual_commits = replay
            .records
            .records()
            .only(&[WalRecordType::CommitTxn])
            .map(|record| record.unwrap().lsn)
            .collect::<Vec<_>>();
        assert_eq!(actual_commits, expected_commits);
        // Independent positioned cursors also permit interleaved passes.
        let mut left = replay.records.records();
        let mut right = replay.records.records();
        assert_eq!(
            left.next().unwrap().unwrap().lsn,
            right.next().unwrap().unwrap().lsn
        );
        assert_eq!(
            left.next_back().unwrap().unwrap().lsn,
            expected.records.last().unwrap().lsn
        );
        assert_eq!(left.next().unwrap().unwrap().lsn, expected.records[1].lsn);
        let mut writer = LogManager::open_from_replay(dir.path(), &replay).unwrap();
        writer
            .append(99, WalPayload::BeginTxn { start_scn: 99 })
            .unwrap();
        drop(writer);
        assert_eq!(replay.records.records().count(), expected.records.len());
        assert!(replay.records.find_lsn(replay.next_lsn).unwrap().is_none());
        let selected = replay.select(&[WalRecordType::CommitTxn]).unwrap();
        assert_eq!(
            selected
                .records()
                .map(|r| r.unwrap().lsn)
                .collect::<Vec<_>>(),
            expected_commits
        );
    }

    #[test]
    fn replay_snapshot_survives_checkpoint_inode_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        log.set_max_segment_bytes(2048);
        for txn in 1..20 {
            log.append(txn, WalPayload::BeginTxn { start_scn: txn })
                .unwrap();
            log.append(
                txn,
                WalPayload::FileCreate {
                    path: "objects/test.heap".into(),
                    after: vec![txn as u8; 512],
                },
            )
            .unwrap();
        }
        log.flush().unwrap();
        let snapshot = LogManager::scan_replay(dir.path()).unwrap();
        let expected = snapshot
            .records
            .records()
            .map(|r| encode_record(&r.unwrap()).unwrap())
            .collect::<Vec<_>>();
        let next = log.next_lsn();
        log.compact_to_checkpoint(next, 50, 100).unwrap();
        assert_eq!(LogManager::scan(dir.path()).unwrap().records.len(), 1);
        let actual = snapshot
            .records
            .records()
            .map(|r| encode_record(&r.unwrap()).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn replay_bootstrap_matches_full_scan_across_terminal_and_empty_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        log.append(1, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        log.rotate_segment().unwrap();
        log.append(
            1,
            WalPayload::CommitTxn {
                commit_scn: 2,
                commit_unix_us: 0,
                binlog: None,
            },
        )
        .unwrap();
        log.append(2, WalPayload::BeginTxn { start_scn: 3 })
            .unwrap();
        log.rotate_segment().unwrap();
        drop(log);
        let scan = LogManager::scan(dir.path()).unwrap();
        let replay = LogManager::scan_replay(dir.path()).unwrap();
        let old = LogManager::open_from_scan_ref(dir.path(), &scan).unwrap();
        let new = LogManager::open_from_replay(dir.path(), &replay).unwrap();
        assert_eq!(old.stats, new.stats);
        assert_eq!(old.txn_last_lsn, new.txn_last_lsn);
        assert_eq!(old.active_info, new.active_info);
        assert_eq!(old.segments, new.segments);
        assert_eq!(old.next_lsn(), new.next_lsn());
    }

    #[test]
    fn replay_refresh_matches_rescan_after_appends_and_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        log.append(1, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        log.flush().unwrap();
        let mut snapshot = LogManager::scan_replay(dir.path()).unwrap();
        for i in 0..3 {
            log.append(
                1,
                WalPayload::FileCreate {
                    path: format!("objects/{i}.heap"),
                    after: vec![i as u8; 32],
                },
            )
            .unwrap();
            log.rotate_segment().unwrap();
        }
        log.rotate_segment().unwrap();
        log.append(1, WalPayload::EndTxn).unwrap();
        log.append(99, WalPayload::BeginTxn { start_scn: 10 })
            .unwrap();
        log.flush().unwrap();
        snapshot.refresh(dir.path()).unwrap();
        let expected = LogManager::scan(dir.path()).unwrap();
        assert_eq!(snapshot.next_lsn, expected.next_lsn);
        assert_eq!(snapshot.end_offset, expected.end_offset);
        assert_eq!(
            snapshot
                .records
                .records()
                .map(|r| encode_record(&r.unwrap()).unwrap())
                .collect::<Vec<_>>(),
            expected
                .records
                .iter()
                .map(|r| encode_record(r).unwrap())
                .collect::<Vec<_>>()
        );
        assert!(!snapshot.records.bootstrap.txn_last_lsn.contains_key(&1));
        let fresh = LogManager::scan_replay(dir.path()).unwrap();
        assert_eq!(
            snapshot.records.bootstrap.txn_bounds,
            fresh.records.bootstrap.txn_bounds
        );
        assert_eq!(
            snapshot.records.bootstrap.segments,
            fresh.records.bootstrap.segments
        );
        assert_eq!(
            snapshot.records.bootstrap.txn_last_lsn,
            fresh.records.bootstrap.txn_last_lsn
        );
        snapshot.refresh(dir.path()).unwrap();
        assert_eq!(snapshot.records.len(), expected.records.len());
    }

    #[test]
    fn replay_decode_refuses_unbounded_allocation() {
        assert!(replay_decode_charge(usize::MAX).is_err());
    }

    #[test]
    fn replay_snapshot_rejects_out_of_range_locations() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        log.append(1, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        drop(log);
        let replay = LogManager::scan_replay(dir.path()).unwrap();
        assert!(replay.records.record(replay.records.len()).is_err());
    }
}
