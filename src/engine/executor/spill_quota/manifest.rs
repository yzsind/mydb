//! Query-owned completion metadata for private temporary files. The manifest
//! does not alter their layout (some consumers seek or mmap raw lanes).

use super::*;
use crate::engine::executor::mem_budget::{self, ScopedCharge};
use crate::engine::executor::memory_footprint::hash_table_capacity_bytes;

type Entry = (PathBuf, Arc<FileRecord>);
pub(super) const CHECKSUM_BLOCK_BYTES: usize = 32 * 1024;

pub(super) struct FileRegistry {
    files: hashbrown::HashMap<PathBuf, Arc<FileRecord>, ahash::RandomState>,
    table_charge: ScopedCharge,
}

impl FileRegistry {
    pub(super) fn new(owner: Option<Arc<mem_budget::MemBudget>>) -> Self {
        Self {
            files: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
            table_charge: ScopedCharge::for_budget(owner),
        }
    }

    pub(super) fn get(&self, path: &Path) -> Option<Arc<FileRecord>> {
        self.files.get(path).cloned()
    }

    pub(super) fn request_writeback(&self, budget: &mut WritebackBudget, _lease: &SpillFileLease) {
        for (path, record) in &self.files {
            if budget.bytes == 0 || budget.files == 0 {
                break;
            }
            let Some(mut submitted) = record.writeback_length.try_lock() else {
                continue;
            };
            let length = record.charged.load(Ordering::Acquire);
            if length == 0 || *submitted == length {
                continue;
            }
            budget.files -= 1;
            // One larger inode may consume the remaining byte budget. The
            // kernel visits dirty pages only; the descriptor count is bounded.
            budget.bytes = budget
                .bytes
                .saturating_sub(length.saturating_sub(*submitted));
            if let Ok(file) = OpenOptions::new().read(true).write(true).open(path) {
                SPILL_FILE_OPENS.fetch_add(1, Ordering::Relaxed);
                if crate::engine::executor::spill_hint::hint_writeback(&file, 0, length) {
                    *submitted = length;
                }
            }
        }
    }

    fn reserve_memory(
        &self,
        charge: &mut ScopedCharge,
        bytes: usize,
        charger: &SpillCharger,
    ) -> Result<(), QueryError> {
        if charge.add_force_recoverable(bytes).is_ok() {
            return Ok(());
        }
        // Global admission deliberately skips this already-locked manifest.
        // Its writer's existing lease pins the directory while another short
        // lease covers the one descriptor used by these writeback requests.
        if crate::engine::memory_reclaimer::pending_writeback_pressure_bytes() != 0 {
            if let Ok(lease) = charger.acquire_file() {
                self.request_writeback(&mut WritebackBudget::new(), &lease);
            }
        }
        charge.add_force(bytes)
    }

    pub(super) fn register(
        &mut self,
        path: &Path,
        charger: &SpillCharger,
    ) -> Result<Arc<FileRecord>, QueryError> {
        if let Some(record) = self.get(path) {
            return Ok(record);
        }
        // A rehash may temporarily retain both allocations. Removal may leave
        // tombstones; never infer a smaller allocation from capacity afterward.
        if self.files.len() == self.files.capacity() {
            let capacity_bound = self.files.len().saturating_add(1).saturating_mul(2).max(4);
            let replacement = hash_table_capacity_bytes::<Entry>(capacity_bound);
            let mut overlap = self.table_charge.sibling();
            self.reserve_memory(&mut overlap, replacement, charger)?;
            self.files
                .try_reserve(1)
                .map_err(|_| mem_budget::allocation_error("spill file manifest table"))?;
            let required = hash_table_capacity_bytes::<Entry>(self.files.capacity());
            let extra = required.saturating_sub(self.table_charge.charged());
            let mut retained = overlap.split(extra);
            self.table_charge.absorb(&mut retained);
        }
        let mut charge = self.table_charge.sibling();
        self.reserve_memory(
            &mut charge,
            std::mem::size_of::<FileRecord>()
                .saturating_add(2 * std::mem::size_of::<usize>())
                .saturating_add(path.as_os_str().len()),
            charger,
        )?;
        let mut key = PathBuf::new();
        key.try_reserve_exact(path.as_os_str().len())
            .map_err(|_| mem_budget::allocation_error("spill file manifest path"))?;
        key.push(path);
        self.reserve_memory(
            &mut charge,
            key.capacity().saturating_sub(path.as_os_str().len()),
            charger,
        )?;
        let state = FileState::new(charge.sibling());
        let record = Arc::new(FileRecord {
            charger: Arc::downgrade(&charger.inner),
            charged: AtomicU64::new(0),
            state: parking_lot::Mutex::new(state),
            writeback_length: parking_lot::Mutex::new(0),
            _memory: charge,
        });
        self.files.insert(key, record.clone());
        Ok(record)
    }

    pub(super) fn remove(&mut self, path: &Path) -> Option<Arc<FileRecord>> {
        self.files.remove(path)
    }

    pub(super) fn clear(&mut self) {
        for record in self.files.values() {
            record.charged.store(0, Ordering::Release);
        }
        self.files = hashbrown::HashMap::with_hasher(ahash::RandomState::new());
        self.table_charge.release_bytes(self.table_charge.charged());
    }
}

struct FileState {
    length: u64,
    tail_hash: crc32fast::Hasher,
    blocks: Vec<u32>,
    block_charge: ScopedCharge,
    finished: bool,
    dirty: bool,
    writing: bool,
    readers: usize,
}

impl FileState {
    fn new(block_charge: ScopedCharge) -> Self {
        Self {
            length: 0,
            tail_hash: crc32fast::Hasher::new(),
            blocks: Vec::new(),
            block_charge,
            finished: false,
            dirty: false,
            writing: false,
            readers: 0,
        }
    }

    fn append_hashes(&mut self, mut position: u64, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            let offset = (position % CHECKSUM_BLOCK_BYTES as u64) as usize;
            if offset == 0 {
                self.tail_hash = crc32fast::Hasher::new();
                self.blocks.push(0);
            }
            let n = (CHECKSUM_BLOCK_BYTES - offset).min(bytes.len());
            self.tail_hash.update(&bytes[..n]);
            *self.blocks.last_mut().expect("current checksum block") =
                self.tail_hash.clone().finalize();
            bytes = &bytes[n..];
            position += n as u64;
        }
    }
}

pub(super) struct FileRecord {
    // Weak avoids a directory -> manifest -> directory ownership cycle.
    charger: std::sync::Weak<ChargerInner>,
    charged: AtomicU64,
    state: parking_lot::Mutex<FileState>,
    /// Last size submitted by admission, independent from the checksum-state
    /// latch so pressure relief remains available during checksum growth.
    writeback_length: parking_lot::Mutex<u64>,
    _memory: ScopedCharge,
}

impl FileRecord {
    pub(super) fn begin_write(&self, append: bool, length: u64) -> io::Result<()> {
        let mut state = self.state.lock();
        if state.writing || state.readers != 0 {
            return Err(invalid("file already has an active reader or writer"));
        }
        if append {
            if length != state.length {
                return Err(invalid("append length differs from manifest"));
            }
        }
        state.finished = false;
        state.writing = true;
        Ok(())
    }

    pub(super) fn truncated(&self) {
        let mut state = self.state.lock();
        let charge = state.block_charge.sibling();
        *state = FileState::new(charge);
        state.writing = true;
        if let Some(inner) = self.charger.upgrade() {
            SpillCharger { inner }.release(self.charged.swap(0, Ordering::AcqRel));
        }
        *self.writeback_length.lock() = 0;
    }

    pub(super) fn end_write(&self) {
        self.state.lock().writing = false;
    }

    pub(super) fn ensure_not_writing(&self) -> io::Result<()> {
        if self.state.lock().writing {
            Err(invalid("cannot remove a file with an active writer"))
        } else {
            Ok(())
        }
    }

    pub(super) fn check_append_length(&self, length: u64) -> io::Result<()> {
        if self.state.lock().length != length {
            Err(invalid("append length differs from manifest"))
        } else {
            Ok(())
        }
    }

    pub(super) fn wrote(&self, position: u64, bytes: &[u8], extension: u64) {
        self.charged.fetch_add(extension, Ordering::AcqRel);
        let mut state = self.state.lock();
        if position < state.length {
            *self.writeback_length.lock() = 0;
        }
        if position == state.length && !state.dirty {
            state.append_hashes(position, bytes);
        } else {
            // Private sort-key headers may be patched after their first row.
            state.dirty = true;
        }
        state.length = state
            .length
            .max(position.saturating_add(bytes.len() as u64));
        state.finished = false;
    }

    /// Reserve checksum capacity before the physical write can extend a file.
    pub(super) fn reserve_write(&self, position: u64, bytes: usize) -> Result<(), QueryError> {
        let mut state = self.state.lock();
        let end = position.saturating_add(bytes as u64).max(state.length);
        let required = usize::try_from(end.div_ceil(CHECKSUM_BLOCK_BYTES as u64))
            .map_err(|_| mem_budget::allocation_error("spill checksum index length"))?;
        if required > state.blocks.capacity() {
            let target = required
                .max(state.blocks.capacity().saturating_mul(2))
                .max(4);
            let FileState {
                blocks,
                block_charge,
                ..
            } = &mut *state;
            mem_budget::try_reserve_vec_hard(
                blocks,
                target.saturating_sub(blocks.len()),
                block_charge,
                "spill checksum index",
            )?;
        }
        Ok(())
    }

    pub(super) fn seal(&self, file: &mut File) -> io::Result<()> {
        let mut state = self.state.lock();
        if file.metadata()?.len() != state.length {
            return Err(invalid("completed length differs from manifest"));
        }
        if state.dirty {
            // Only backpatched files need a reread. Ordinary append-only data
            // are checksummed once per physical write, below BufWriter.
            let position = file.stream_position()?;
            file.seek(SeekFrom::Start(0))?;
            state.blocks.clear();
            state.tail_hash = crc32fast::Hasher::new();
            let mut remaining = state.length;
            let mut offset = 0;
            let mut buffer = [0; 8192];
            while remaining != 0 {
                crate::engine::cancel::check_cancelled().map_err(quota_as_io)?;
                let n = remaining.min(buffer.len() as u64) as usize;
                file.read_exact(&mut buffer[..n])?;
                crate::engine::executor::spill_hint::record_read(n as u64);
                state.append_hashes(offset, &buffer[..n]);
                remaining -= n as u64;
                offset += n as u64;
            }
            file.seek(SeekFrom::Start(position))?;
            state.dirty = false;
        }
        state.finished = true;
        Ok(())
    }

    pub(super) fn acquire_read(self: &Arc<Self>, length: u64) -> io::Result<FileReadLease> {
        let mut state = self.state.lock();
        if length != state.length {
            return Err(invalid("file length differs from completed manifest"));
        }
        if state.writing {
            return Err(invalid("file still has an active writer"));
        }
        if !state.finished
            || state.blocks.len() as u64 != length.div_ceil(CHECKSUM_BLOCK_BYTES as u64)
        {
            return Err(invalid("file has no completion marker"));
        }
        state.readers += 1;
        Ok(FileReadLease(self.clone()))
    }

    pub(super) fn validate_blocks(&self, start: u64, bytes: &[u8]) -> io::Result<()> {
        let state = self.state.lock();
        let first = usize::try_from(start / CHECKSUM_BLOCK_BYTES as u64)
            .map_err(|_| invalid("checksum block offset overflow"))?;
        for (offset, block) in bytes.chunks(CHECKSUM_BLOCK_BYTES).enumerate() {
            if state.blocks.get(first + offset).copied() != Some(crc32fast::hash(block)) {
                return Err(invalid("block checksum mismatch"));
            }
        }
        Ok(())
    }
}

pub(super) struct FileReadLease(Arc<FileRecord>);

impl std::ops::Deref for FileReadLease {
    type Target = FileRecord;
    fn deref(&self) -> &FileRecord {
        &self.0
    }
}

impl Drop for FileReadLease {
    fn drop(&mut self) {
        self.0.state.lock().readers -= 1;
    }
}

impl Drop for FileRecord {
    fn drop(&mut self) {
        if let Some(inner) = self.charger.upgrade() {
            SpillCharger { inner }.release(self.charged.swap(0, Ordering::AcqRel));
        }
    }
}

pub(super) fn invalid(reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid spill file: {reason}"),
    )
}

impl SpillCharger {
    pub(super) fn register_file(&self, path: &Path) -> Result<Arc<FileRecord>, QueryError> {
        self.inner.files.lock().register(path, self)
    }

    pub(super) fn file_record(&self, path: &Path) -> Result<Arc<FileRecord>, QueryError> {
        self.inner.files.lock().get(path).ok_or_else(|| {
            crate::error::catalog::ER_UNKNOWN_ERROR.error(format!(
                "spill file '{}' has no registered owner",
                path.display()
            ))
        })
    }

    /// A missing lazy partition is empty only if no file was ever created.
    pub(crate) fn absent_partition(&self, path: &Path) -> Result<bool, QueryError> {
        match std::fs::metadata(path) {
            Ok(_) => Ok(false),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if self.inner.files.lock().get(path).is_some() {
                    return Err(crate::error::catalog::ER_UNKNOWN_ERROR.error(format!(
                        "registered spill file '{}' is missing",
                        path.display()
                    )));
                }
                Ok(true)
            }
            Err(error) => Err(crate::error::catalog::ER_UNKNOWN_ERROR
                .error(format!("spill metadata '{}': {error}", path.display()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writeback_admission_resubmits_backpatches_and_truncated_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cursor.bin");
        let charger = charger_for_new_dir(dir.path()).unwrap();
        let mut writer = TrackedSpillWriter::create_new(&path, charger.clone(), 32 * 1024).unwrap();
        writer.write_all(b"original").unwrap();
        writer.flush().unwrap();
        let record = charger.file_record(&path).unwrap();
        *record.writeback_length.lock() = 8;
        writer.seek(SeekFrom::Start(0)).unwrap();
        writer.write_all(b"O").unwrap();
        writer.flush().unwrap();
        assert_eq!(*record.writeback_length.lock(), 0);
        writer.finish().unwrap();
        *record.writeback_length.lock() = 8;
        let mut writer =
            TrackedSpillWriter::create_truncate(&path, charger.clone(), 32 * 1024).unwrap();
        assert_eq!(record.charged.load(Ordering::Acquire), 0);
        assert_eq!(*record.writeback_length.lock(), 0);
        writer.write_all(b"new").unwrap();
        writer.finish().unwrap();
        let mut reader = TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, b"new");
        drop(reader);
        drop(record);
        release_dir(&charger);
    }
}
