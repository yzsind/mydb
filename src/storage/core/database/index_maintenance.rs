use crate::engine::executor::mem_budget::load_data_ancillary_headroom as load_data_index_ancillary_headroom;
use crate::engine::executor::mem_budget::load_data_reclaim_target as load_data_index_reclaim_target_for_values;
static NEXT_INDEX_SORT_TMP_SEQ: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_DICTIONARY_BUILDS: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_DICTIONARY_ROWS: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_ENCODED_RUNS: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_SCRATCH_GRANTED_PEAK: AtomicU64 = AtomicU64::new(0);

pub(crate) fn index_build_extended_perf_snapshot() -> [u64; 4] {
    [
        INDEX_BUILD_DICTIONARY_BUILDS.load(Ordering::Relaxed),
        INDEX_BUILD_DICTIONARY_ROWS.load(Ordering::Relaxed),
        INDEX_BUILD_ENCODED_RUNS.load(Ordering::Relaxed),
        INDEX_BUILD_SCRATCH_GRANTED_PEAK.load(Ordering::Relaxed),
    ]
}

/// Reject an explicit NULL in a required non-AUTO_INCREMENT column before
/// reserving an AUTO_INCREMENT value. MySQL performs this deterministic
/// row-shape validation before allocation, so a failed `INSERT (..., NULL)`
/// does not create a sequence gap. Generated columns are materialized later
/// and are therefore excluded from this preflight.
fn preflight_not_null_row_before_auto_increment(
    table: &TableSchema,
    values: &[Value],
) -> Result<(), QueryError> {
    for (index, column) in table.columns.iter().enumerate() {
        if column.state == ColumnState::Dropped
            || column.nullable
            || column.auto_increment
            || column.generated_expr.is_some()
            || !matches!(values.get(index), Some(Value::Null))
        {
            continue;
        }
        return Err(crate::error::messages::bad_null(&column.name));
    }
    Ok(())
}

enum SortedIndexEntries {
    Memory(Vec<IndexEntry>),
    Spilled(IndexEntryRunSet),
}

struct IndexEntryRunSet {
    tmp: IndexSortTmpDir,
    runs: Vec<PathBuf>,
}

impl IndexEntryRunSet {
    fn into_merger(self) -> Result<IndexEntryRunMerger, QueryError> {
        IndexEntryRunMerger::open(self)
    }
}

struct IndexSortTmpDir {
    path: PathBuf,
}

impl IndexSortTmpDir {
    fn create(data_dir: &Path) -> Result<Self, QueryError> {
        let parent = crate::storage::naming::system_tmp_dir(data_dir);
        fs::create_dir_all(&parent).map_err(io_err)?;
        let pid = std::process::id();
        let seq = NEXT_INDEX_SORT_TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!("indexsort-{pid}-{seq}"));
        fs::create_dir(&path).map_err(io_err)?;
        Ok(Self { path })
    }
}

impl Drop for IndexSortTmpDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct IndexSpillReadFile(File);

impl Read for IndexSpillReadFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let started = Instant::now();
        let result = self.0.read(buf);
        INDEX_BUILD_SPILL_READ_US.fetch_add(
            started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        result
    }
}

struct IndexSpillWriteFile(File);

impl Write for IndexSpillWriteFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let started = Instant::now();
        let result = self.0.write(buf);
        INDEX_BUILD_SPILL_WRITE_US.fetch_add(
            started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        result
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let started = Instant::now();
        let result = self.0.flush();
        INDEX_BUILD_SPILL_WRITE_US.fetch_add(
            started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        result
    }
}

struct IndexBuildSortTimer(Instant);

impl IndexBuildSortTimer {
    fn start() -> Self {
        Self(Instant::now())
    }
}

impl Drop for IndexBuildSortTimer {
    fn drop(&mut self) {
        INDEX_BUILD_SORT_US.fetch_add(
            self.0.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }
}

struct IndexEntryRunWriter {
    writer: std::io::BufWriter<IndexSpillWriteFile>,
}

impl IndexEntryRunWriter {
    fn create(path: &Path) -> Result<Self, QueryError> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .map_err(io_err)?;
        Ok(Self {
            writer: std::io::BufWriter::new(IndexSpillWriteFile(file)),
        })
    }

    fn append(&mut self, entry: &IndexEntry) -> Result<(), QueryError> {
        // Stream format is u32 part_count + key parts + full 20B
        // RowId — independent of the v10 .idx page format, so
        // `encoded_index_entry_run_stream_len` (not the page-format
        // `encoded_index_entry_len`) sizes the buffer exactly.
        let len = encoded_index_entry_run_stream_len(entry)?;
        let mut buf = vec![0; len];
        let mut offset = 0usize;
        // External-sort run files are stream-shaped (no page header
        // → no page-level table_id to fold), so use the full-RowId
        // form rather than the paginated v7 encoder.
        encode_index_entry_for_run_stream(entry, &mut buf, &mut offset)?;
        self.writer
            .write_all(&(offset as u32).to_le_bytes())
            .map_err(io_err)?;
        self.writer.write_all(&buf[..offset]).map_err(io_err)
    }

    fn finish(mut self) -> Result<(), QueryError> {
        self.writer.flush().map_err(io_err)
    }
}

struct IndexEntryRunReader {
    reader: std::io::BufReader<IndexSpillReadFile>,
}

impl IndexEntryRunReader {
    fn open(path: &Path) -> Result<Self, QueryError> {
        let file = File::open(path).map_err(io_err)?;
        Ok(Self {
            reader: std::io::BufReader::new(IndexSpillReadFile(file)),
        })
    }

    fn next_entry(&mut self) -> Result<Option<IndexEntry>, QueryError> {
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(io_err(e)),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0; len];
        self.reader.read_exact(&mut buf).map_err(io_err)?;
        let mut offset = 0usize;
        decode_index_entry_from_run_stream(&buf, &mut offset).map(Some)
    }
}

/// Tournament over the current head of each sorted run.
///
/// A binary heap performs a pop and a push for every emitted entry.  The
/// tournament keeps the same winner tree in place and only repairs the path
/// belonging to the run that advanced.  Besides lowering the comparison and
/// move count, this one implementation is shared by generic, compact and
/// pair-INT external merges.
struct RunHeadTournament<E> {
    heads: Vec<Option<E>>,
    tree: Vec<usize>,
    leaf_base: usize,
}

impl<E: Ord> RunHeadTournament<E> {
    const EMPTY: usize = usize::MAX;

    fn new(heads: Vec<Option<E>>) -> Self {
        let leaf_base = heads.len().max(1).next_power_of_two();
        let mut tournament = Self {
            heads,
            tree: vec![Self::EMPTY; leaf_base * 2],
            leaf_base,
        };
        for run in 0..tournament.heads.len() {
            if tournament.heads[run].is_some() {
                tournament.tree[leaf_base + run] = run;
            }
        }
        for node in (1..leaf_base).rev() {
            tournament.recompute(node);
        }
        tournament
    }

    #[inline]
    fn winner(&self) -> Option<usize> {
        (self.tree[1] != Self::EMPTY).then_some(self.tree[1])
    }

    fn replace(&mut self, run: usize, next: Option<E>) -> Option<E> {
        let previous = std::mem::replace(&mut self.heads[run], next);
        let mut node = self.leaf_base + run;
        self.tree[node] = if self.heads[run].is_some() {
            run
        } else {
            Self::EMPTY
        };
        while node > 1 {
            node /= 2;
            self.recompute(node);
        }
        previous
    }

    #[inline]
    fn recompute(&mut self, node: usize) {
        let left = self.tree[node * 2];
        let right = self.tree[node * 2 + 1];
        self.tree[node] = self.better(left, right);
    }

    #[inline]
    fn better(&self, left: usize, right: usize) -> usize {
        match (left, right) {
            (Self::EMPTY, other) | (other, Self::EMPTY) => other,
            (left, right) => {
                let left_head = self.heads[left].as_ref().expect("live tournament head");
                let right_head = self.heads[right].as_ref().expect("live tournament head");
                if left_head < right_head || (left_head == right_head && left < right) {
                    left
                } else {
                    right
                }
            }
        }
    }
}

struct IndexEntryRunMerger {
    _tmp: IndexSortTmpDir,
    readers: Vec<IndexEntryRunReader>,
    tournament: RunHeadTournament<IndexEntry>,
    last_emitted: Option<IndexEntry>,
}

/// Validate UNIQUE/PRIMARY semantics while a spill merge is consumed by the
/// direct B-tree bulk loader.  Keeping the check in the ordered stream removes
/// ALTER COPY's former whole-table HashSet and its second validation scan.
struct UniqueCheckedIndexEntries<I> {
    inner: I,
    previous: Option<IndexKey>,
    index_name: String,
    primary: bool,
}

impl<I> UniqueCheckedIndexEntries<I> {
    fn new(inner: I, index: &IndexSchema) -> Self {
        Self {
            inner,
            previous: None,
            index_name: index.name.clone(),
            primary: index.primary,
        }
    }
}

impl<I> Iterator for UniqueCheckedIndexEntries<I>
where
    I: Iterator<Item = Result<IndexEntry, QueryError>>,
{
    type Item = Result<IndexEntry, QueryError>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = match self.inner.next()? {
            Ok(entry) => entry,
            Err(err) => return Some(Err(err)),
        };
        let has_null = entry.key.0.iter().any(|part| matches!(part, KeyPart::Null));
        if self.primary && has_null {
            return Some(Err(error(
                crate::error::catalog::ER_INVALID_USE_OF_NULL,
                "invalid use of NULL value for primary key",
            )));
        }
        let nullable_unique = !self.primary && has_null;
        if !nullable_unique && self.previous.as_ref() == Some(&entry.key) {
            return Some(Err(error(
                crate::error::catalog::ER_DUP_ENTRY,
                format!("duplicate entry for key '{}'", self.index_name),
            )));
        }
        self.previous = (!nullable_unique).then(|| entry.key.clone());
        Some(Ok(entry))
    }
}

fn validate_unique_memory_entries(
    entries: &[IndexEntry],
    index: &IndexSchema,
) -> Result<(), QueryError> {
    if !index.unique {
        return Ok(());
    }
    let mut previous: Option<&IndexKey> = None;
    for entry in entries {
        let has_null = entry.key.0.iter().any(|part| matches!(part, KeyPart::Null));
        if has_null {
            if index.primary {
                return Err(error(
                    crate::error::catalog::ER_INVALID_USE_OF_NULL,
                    "invalid use of NULL value for primary key",
                ));
            }
            previous = None;
            continue;
        }
        if previous == Some(&entry.key) {
            return Err(error(
                crate::error::catalog::ER_DUP_ENTRY,
                format!("duplicate entry for key '{}'", index.name),
            ));
        }
        previous = Some(&entry.key);
    }
    Ok(())
}

impl IndexEntryRunMerger {
    fn open(run_set: IndexEntryRunSet) -> Result<Self, QueryError> {
        let IndexEntryRunSet { tmp, runs } = run_set;
        let runs =
            consolidate_sort_runs::<IndexEntryRunCodec>(&tmp.path, runs, index_merge_fan_in())?;
        let mut readers = Vec::with_capacity(runs.len());
        let mut heads = Vec::with_capacity(runs.len());
        for path in runs {
            if let Ok(meta) = fs::metadata(&path) {
                record_spill_bytes_read(meta.len());
            }
            let mut reader = IndexEntryRunReader::open(&path)?;
            let Some(entry) = reader.next_entry()? else {
                continue;
            };
            readers.push(reader);
            heads.push(Some(entry));
        }
        Ok(Self {
            _tmp: tmp,
            readers,
            tournament: RunHeadTournament::new(heads),
            last_emitted: None,
        })
    }

    fn next_entry(&mut self) -> Result<Option<IndexEntry>, QueryError> {
        while let Some(run) = self.tournament.winner() {
            let next = self.readers[run].next_entry()?;
            let entry = self
                .tournament
                .replace(run, next)
                .expect("winner has a live run head");
            if self.last_emitted.as_ref() == Some(&entry) {
                continue;
            }
            self.last_emitted = Some(entry.clone());
            return Ok(Some(entry));
        }
        Ok(None)
    }
}

impl Iterator for IndexEntryRunMerger {
    type Item = Result<IndexEntry, QueryError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_entry() {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

struct IndexEntryExternalSorter {
    tmp: Option<IndexSortTmpDir>,
    runs: Vec<PathBuf>,
    entries: Vec<IndexEntry>,
    bytes: usize,
    run_bytes: usize,
}

impl IndexEntryExternalSorter {
    /// Hard-budget constructor for governed LOAD/DDL index builds.
    fn with_exact_run_bytes(run_bytes: usize) -> Self {
        // `run_bytes` is the complete peak budget, including Vec spare
        // capacity and parallel merge scratch. Keep the initialized source
        // payload below half and preallocate exactly that many inline entries:
        //
        //   source Vec capacity + out-of-line key payload + merge Vec
        //   <= B/2              + payload                 + live inline bytes
        //   <= B
        //
        // This makes the owner's reservation capacity-accurate even when Vec
        // would otherwise grow geometrically near the spill watermark.
        let run_bytes = run_bytes.max(1024 * 1024);
        let source_bytes = run_bytes / 2;
        Self {
            tmp: None,
            runs: Vec::new(),
            entries: Vec::with_capacity(
                source_bytes
                    .saturating_div(std::mem::size_of::<IndexEntry>())
                    .max(1),
            ),
            bytes: 0,
            run_bytes: source_bytes,
        }
    }

    fn push(&mut self, data_dir: &Path, entry: IndexEntry) -> Result<(), QueryError> {
        self.push_with_sort_degree(data_dir, entry, None)
    }

    fn push_with_sort_degree(
        &mut self,
        data_dir: &Path,
        entry: IndexEntry,
        degree: Option<usize>,
    ) -> Result<(), QueryError> {
        let entry_bytes = estimate_index_entry_sort_bytes(&entry);
        if !self.entries.is_empty() && self.bytes.saturating_add(entry_bytes) > self.run_bytes {
            match degree {
                Some(degree) => self.flush_run_with_degree(data_dir, degree)?,
                None => self.flush_run(data_dir)?,
            }
        }
        self.bytes = self.bytes.saturating_add(entry_bytes);
        self.entries.push(entry);
        Ok(())
    }

    fn finish(self, data_dir: &Path) -> Result<SortedIndexEntries, QueryError> {
        self.finish_with_degree(data_dir, index_sort_parallel_degree())
    }

    fn finish_with_degree(
        mut self,
        data_dir: &Path,
        sort_degree: usize,
    ) -> Result<SortedIndexEntries, QueryError> {
        if self.runs.is_empty() {
            // Parallel sort + dedup: 4 workers on 6 M lineitem
            // indexes cuts the in-memory sort from ~750 ms to
            // ~280 ms on a generic 4-core box. Degree is bounded
            // by the user's `MYDB_PARALLEL_DEGREE` to avoid
            // over-subscribing shared hosts.
            {
                let _sort_timer = IndexBuildSortTimer::start();
                if self.entries.windows(2).any(|pair| pair[0] > pair[1]) {
                    sort_index_entries(&mut self.entries, sort_degree);
                } else {
                    self.entries.dedup();
                }
            }
            return Ok(SortedIndexEntries::Memory(self.entries));
        }
        if !self.entries.is_empty() {
            self.flush_run_with_degree(data_dir, sort_degree)?;
        }
        let tmp = self
            .tmp
            .take()
            .expect("index sort temp dir should exist after spilling");
        Ok(SortedIndexEntries::Spilled(IndexEntryRunSet {
            tmp,
            runs: self.runs,
        }))
    }

    fn flush_run(&mut self, data_dir: &Path) -> Result<(), QueryError> {
        self.flush_run_with_degree(data_dir, index_sort_parallel_degree())
    }

    fn flush_run_with_degree(
        &mut self,
        data_dir: &Path,
        sort_degree: usize,
    ) -> Result<(), QueryError> {
        if self.entries.is_empty() {
            return Ok(());
        }
        if self.runs.is_empty() {
            let tmp = IndexSortTmpDir::create(data_dir)?;
            self.tmp = Some(tmp);
        }
        {
            let _sort_timer = IndexBuildSortTimer::start();
            sort_index_entries(&mut self.entries, sort_degree);
        }
        let tmp = self
            .tmp
            .as_ref()
            .expect("index sort temp dir should exist when writing runs");
        let path = tmp.path.join(format!("run-{}", self.runs.len()));
        let mut writer = IndexEntryRunWriter::create(&path)?;
        for entry in self.entries.drain(..) {
            writer.append(&entry)?;
        }
        writer.finish()?;
        if let Ok(meta) = fs::metadata(&path) {
            record_spill_bytes_written(meta.len());
        }
        self.bytes = 0;
        self.runs.push(path);
        Ok(())
    }
}

/// Bounded index fan-out for heap-only `LOAD DATA`.
///
/// The old loader retained `Vec<(IndexKey, RowId)>` for every index until EOF.
/// `IndexKey` deliberately keeps four `KeyPart`s inline, so a two-column
/// primary-key entry is currently 240 bytes including its `RowId`; a 30 M-row
/// load therefore pinned at least 6.7 GiB outside the memory governor.  Each
/// plan below owns one external sorter and a divided `StorageScratch`
/// reservation. Sorted runs spill once their share fills, while the final
/// merge still feeds the bottom-up B-tree stream builder.
pub(crate) enum LoadDataIndexKeyBatch {
    /// Contiguous order-key bytes, one fixed-width key per source row.
    /// The schema program, not a Rust type/arity variant, defines the stride.
    EncodedFixed {
        key_width: usize,
        keys: Vec<u8>,
    },
    Encoded(Vec<smallvec::SmallVec<[EncodedPendingIndexRecord; 1]>>),
    Generic(Vec<smallvec::SmallVec<[IndexKey; 1]>>),
}

pub(crate) struct LoadDataIndexBuilders {
    table_id: u64,
    plans: Vec<LoadDataIndexPlan>,
    /// Live-table builds must WAL-log their logical index feed before page
    /// publication. A private shadow generation is not reachable until its
    /// crash-safe file swap, so its feed is intentionally WAL-free.
    emit_logical_wal: bool,
    _charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

struct LoadDataIndexPlan {
    index: IndexSchema,
    sorter: LoadDataIndexSorter,
    empty_at_start: bool,
    last_lsn: Option<u64>,
}

enum LoadDataIndexSorter {
    Encoded {
        program: Arc<CompiledIndexKeyProgram>,
        sorter: EncodedIndexSorter,
    },
    Generic(IndexEntryExternalSorter),
}

enum LoadDataSortedEntries {
    Encoded {
        program: Arc<CompiledIndexKeyProgram>,
        sorted: EncodedSortedRecords,
    },
    Generic(SortedIndexEntries),
}

// Physical leaf encoders accept these canonical tuple streams. Type/arity
// selection no longer leaks into LOAD or DDL planning.
trait PairIntLeafEntry: Copy {
    fn unpack_pair_int(self) -> ((i64, i64), RowId);
}

impl PairIntLeafEntry for ((i64, i64), RowId) {
    fn unpack_pair_int(self) -> ((i64, i64), RowId) {
        self
    }
}

trait IndexSortRunCodec {
    type Entry: Clone + Ord;
    type Reader;
    type Writer;

    fn open_reader(path: &Path) -> Result<Self::Reader, QueryError>;
    fn next_entry(reader: &mut Self::Reader) -> Result<Option<Self::Entry>, QueryError>;
    fn create_writer(path: &Path) -> Result<Self::Writer, QueryError>;
    fn append(writer: &mut Self::Writer, entry: &Self::Entry) -> Result<(), QueryError>;
    fn finish(writer: Self::Writer) -> Result<(), QueryError>;
}

struct IndexEntryRunCodec;

impl IndexSortRunCodec for IndexEntryRunCodec {
    type Entry = IndexEntry;
    type Reader = IndexEntryRunReader;
    type Writer = IndexEntryRunWriter;

    fn open_reader(path: &Path) -> Result<Self::Reader, QueryError> {
        IndexEntryRunReader::open(path)
    }

    fn next_entry(reader: &mut Self::Reader) -> Result<Option<Self::Entry>, QueryError> {
        reader.next_entry()
    }

    fn create_writer(path: &Path) -> Result<Self::Writer, QueryError> {
        IndexEntryRunWriter::create(path)
    }

    fn append(writer: &mut Self::Writer, entry: &Self::Entry) -> Result<(), QueryError> {
        writer.append(entry)
    }

    fn finish(writer: Self::Writer) -> Result<(), QueryError> {
        writer.finish()
    }
}

fn index_merge_fan_in() -> usize {
    const DESCRIPTOR_HEADROOM: usize = 64;
    const MIN_FAN_IN: usize = 8;
    const MAX_FAN_IN: usize = 128;

    let reserve = usize::try_from(crate::mysql::connection_manager::open_files_reserve())
        .unwrap_or(usize::MAX);
    let available = reserve.saturating_sub(DESCRIPTOR_HEADROOM);
    available
        .saturating_div(index_sort_parallel_degree().max(1))
        .clamp(MIN_FAN_IN, MAX_FAN_IN)
}

fn merge_sort_run_group<C: IndexSortRunCodec>(
    paths: &[PathBuf],
    output: &Path,
) -> Result<(), QueryError> {
    let mut readers = Vec::with_capacity(paths.len());
    let mut heap = BinaryHeap::with_capacity(paths.len());
    for path in paths {
        if let Ok(meta) = fs::metadata(path) {
            record_spill_bytes_read(meta.len());
        }
        let mut reader = C::open_reader(path)?;
        let Some(entry) = C::next_entry(&mut reader)? else {
            continue;
        };
        let run = readers.len();
        readers.push(reader);
        heap.push(Reverse((entry, run)));
    }

    let mut writer = C::create_writer(output)?;
    let mut previous = None;
    while let Some(Reverse((entry, run))) = heap.pop() {
        if let Some(next) = C::next_entry(&mut readers[run])? {
            heap.push(Reverse((next, run)));
        }
        if previous.as_ref() == Some(&entry) {
            continue;
        }
        C::append(&mut writer, &entry)?;
        previous = Some(entry);
    }
    C::finish(writer)?;
    drop(readers);

    if let Ok(meta) = fs::metadata(output) {
        record_spill_bytes_written(meta.len());
    }
    for path in paths {
        fs::remove_file(path).map_err(io_err)?;
    }
    Ok(())
}

fn consolidate_sort_runs<C: IndexSortRunCodec>(
    tmp_dir: &Path,
    mut runs: Vec<PathBuf>,
    fan_in: usize,
) -> Result<Vec<PathBuf>, QueryError> {
    debug_assert!(fan_in >= 2);
    let mut pass = 0usize;
    while runs.len() > fan_in {
        let mut next = Vec::with_capacity(runs.len().div_ceil(fan_in));
        for (group, paths) in runs.chunks(fan_in).enumerate() {
            if paths.len() == 1 {
                next.push(paths[0].clone());
                continue;
            }
            let output = tmp_dir.join(format!("merge-{pass}-{group}"));
            merge_sort_run_group::<C>(paths, &output)?;
            next.push(output);
        }
        runs = next;
        pass += 1;
    }
    Ok(runs)
}

struct ForegroundBulkDdlGuard<'a> {
    active: &'a AtomicU64,
}

struct IndexBuildWorkerGuard;

fn enter_index_build_worker() -> IndexBuildWorkerGuard {
    let active = INDEX_BUILD_ACTIVE_WORKERS.fetch_add(1, Ordering::AcqRel) + 1;
    INDEX_BUILD_PEAK_ACTIVE_WORKERS.fetch_max(active, Ordering::Relaxed);
    IndexBuildWorkerGuard
}

impl Drop for IndexBuildWorkerGuard {
    fn drop(&mut self) {
        let previous = INDEX_BUILD_ACTIVE_WORKERS.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "index-build worker counter underflow");
    }
}

impl Drop for ForegroundBulkDdlGuard<'_> {
    fn drop(&mut self) {
        let previous = self.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "foreground bulk-DDL counter underflow");
    }
}

impl Database {
    fn enter_foreground_bulk_ddl(&self) -> ForegroundBulkDdlGuard<'_> {
        self.foreground_bulk_ddl_active
            .fetch_add(1, Ordering::AcqRel);
        ForegroundBulkDdlGuard {
            active: &self.foreground_bulk_ddl_active,
        }
    }

    fn foreground_bulk_ddl_is_active(&self) -> bool {
        self.foreground_bulk_ddl_active.load(Ordering::Acquire) != 0
    }

    pub(crate) fn load_data_index_builders(
        &self,
        table: &TableSchema,
    ) -> Result<LoadDataIndexBuilders, QueryError> {
        self.load_data_index_builders_for(table, true)
    }

    fn shadow_load_data_index_builders(
        &self,
        table: &TableSchema,
    ) -> Result<LoadDataIndexBuilders, QueryError> {
        self.load_data_index_builders_for(table, false)
    }

    fn load_data_index_builders_for(
        &self,
        table: &TableSchema,
        emit_logical_wal: bool,
    ) -> Result<LoadDataIndexBuilders, QueryError> {
        if table.indexes.is_empty() || table.is_partitioned() {
            return Ok(LoadDataIndexBuilders {
                table_id: table.id,
                plans: Vec::new(),
                emit_logical_wal,
                _charge: None,
            });
        }

        // Reserve the aggregate run/sort budget up front. Four MiB per index
        // is the minimum that leaves room for the source vector, parallel
        // sort's top-level merge Vec, and the k-way reader set. If the managed
        // target cannot provide even that bounded floor, fail with 1037 rather
        // than silently returning to whole-table untracked allocation.
        let minimum = table.indexes.len().saturating_mul(4 * 1024 * 1024);
        // A private-generation sorter shares the statement's executor target
        // with the bounded raw/prepared parallel-LOAD pipeline. Reserving the
        // complete executor target here double-counted that budget: with the
        // default 60/30 split, a full buffer pool + sorter + pipeline could
        // cross MYDB_MEMORY_TARGET before the first batch reached the writer.
        // Keep one query-sized slice for the pipeline and give the remainder
        // to the spillable sorter. The minimum still guarantees a usable run
        // for every index under unusually small/custom executor settings.
        let (requested_budget, _) =
            self.load_data_memory_budgets(table.indexes.len(), emit_logical_wal);
        let (charge, aggregate_budget) =
            reserve_load_data_index_scratch(self, requested_budget.max(minimum), minimum)?;
        INDEX_BUILD_SCRATCH_GRANTED_PEAK.fetch_max(aggregate_budget as u64, Ordering::Relaxed);
        let per_index_budget = aggregate_budget / table.indexes.len();
        // Special-index registries still use the canonical IndexEntry sorter;
        // every B-tree shape uses the compiled encoded sorter below.
        let run_bytes = (per_index_budget / 4).max(1024 * 1024);

        // A private generation replaces files that can still have older
        // initialization WAL. Stamp every page with the captured generation
        // floor even though its rows do not emit logical index WAL.
        let generation_lsn = (!emit_logical_wal).then(|| self.log.lock().last_lsn());
        let mut plans = Vec::with_capacity(table.indexes.len());
        for index in &table.indexes {
            let empty_at_start = index.state == IndexState::Public
                && self.with_index_read(table.id, &index.name, |file| file.is_empty())?;
            let sorter = if encoded_btree_build_eligible(index) {
                let program = Arc::new(CompiledIndexKeyProgram::compile(table, index)?);
                LoadDataIndexSorter::Encoded {
                    program: program.clone(),
                    sorter: EncodedIndexSorter::new(program, per_index_budget),
                }
            } else {
                LoadDataIndexSorter::Generic(IndexEntryExternalSorter::with_exact_run_bytes(
                    run_bytes,
                ))
            };
            plans.push(LoadDataIndexPlan {
                index: {
                    let mut writable = index.clone();
                    writable.unique = index.enforces_uniqueness();
                    writable
                },
                sorter,
                empty_at_start,
                last_lsn: generation_lsn,
            });
        }
        Ok(LoadDataIndexBuilders {
            table_id: table.id,
            plans,
            emit_logical_wal,
            _charge: Some(charge),
        })
    }

    pub(crate) fn load_data_memory_budgets(
        &self,
        sorter_count: usize,
        emit_logical_wal: bool,
    ) -> (usize, usize) {
        let active_loads = self
            .dml_stats
            .load_data_active
            .load(Ordering::Relaxed)
            .max(1) as usize;
        load_data_memory_budgets_for_concurrency(
            sorter_count,
            emit_logical_wal,
            crate::engine::executor::mem_budget::query_mem_limit_bytes(),
            crate::engine::executor::mem_budget::executor_memory_target_bytes(),
            active_loads,
        )
    }
}

/// Admit the spillable index sorter after giving optional buffer residency a
/// chance to yield. LOAD DATA used to halve its run budget down to the 4 MiB
/// floor with hard `try_new` calls; ten concurrent loaders could therefore
/// return 1037 while most of BufferRaw was clean or sitting in the recycled
/// raw-page cache. The sorter is mandatory statement state, so serialize the
/// short reclaim/admission transition and preserve a bounded page-I/O slice.
fn reserve_load_data_index_scratch(
    db: &Database,
    requested: usize,
    minimum: usize,
) -> Result<
    (
        crate::engine::executor::mem_budget::ControlledMemoryCharge,
        usize,
    ),
    QueryError,
> {
    use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};

    let mut granted = requested.max(minimum);
    if let Some(charge) =
        ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::StorageScratch, granted)
    {
        return Ok((charge, granted));
    }

    static ADMISSION: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    let admission = ADMISSION.get_or_init(|| std::sync::Mutex::new(()));
    let _guard = admission
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    loop {
        if let Some(charge) = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::StorageScratch,
            granted,
        ) {
            return Ok((charge, granted));
        }

        let (controlled, _, categories, _) =
            crate::engine::executor::mem_budget::controlled_memory_snapshot();
        let operational_limit =
            crate::engine::executor::mem_budget::operational_memory_limit_bytes();
        let ancillary_headroom = load_data_index_ancillary_headroom(operational_limit);
        let min_buffer_bytes = (16 * 1024 * 1024)
            .min(crate::engine::executor::mem_budget::memory_target_bytes() / 4)
            .max(PAGE_SIZE);
        if let Some(target) = load_data_index_reclaim_target_for_values(
            controlled,
            categories[0],
            granted,
            ancillary_headroom,
            min_buffer_bytes,
            operational_limit,
        ) {
            // A failed reclaim normally means transient pins. It may still
            // have discarded recycled or clean pages, so always retry the
            // optional admission before reducing the spillable run budget.
            let _ = db.reclaim_for_memory_target(target);
            let _ = crate::engine::memory_reclaimer::release_allocator_cached_memory();
            if let Some(charge) = ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::StorageScratch,
                granted,
            ) {
                return Ok((charge, granted));
            }
        }

        if granted > minimum {
            granted = (granted / 2).max(minimum);
            continue;
        }
        // Record exactly one hard rejection, and retain the canonical
        // 1037/HY001 error only after cache reclaim and spill downgrading have
        // both been exhausted.
        return ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, minimum)
            .map(|charge| (charge, minimum));
    }
}

fn load_data_memory_budgets_for_concurrency(
    sorter_count: usize,
    emit_logical_wal: bool,
    query_budget: usize,
    executor_budget: usize,
    active_loads: usize,
) -> (usize, usize) {
    let active_loads = active_loads.max(1);
    load_data_memory_budgets_for_values(
        sorter_count,
        emit_logical_wal,
        query_budget / active_loads,
        executor_budget / active_loads,
    )
}

fn load_data_memory_budgets_for_values(
    sorter_count: usize,
    emit_logical_wal: bool,
    query_budget: usize,
    executor_budget: usize,
) -> (usize, usize) {
    if sorter_count == 0 {
        return (0, query_budget.min(executor_budget));
    }
    let minimum = sorter_count.saturating_mul(4 * 1024 * 1024);
    if emit_logical_wal {
        let sorter_budget = query_budget.max(minimum);
        let pipeline_budget = query_budget.min(executor_budget.saturating_sub(sorter_budget));
        return (sorter_budget, pipeline_budget);
    }
    let pipeline_budget = query_budget.min(executor_budget.saturating_sub(minimum));
    let sorter_budget = executor_budget.saturating_sub(pipeline_budget).max(minimum);
    (sorter_budget, pipeline_budget)
}

#[cfg(test)]
mod load_data_sorter_budget_tests {
    use super::{
        load_data_index_reclaim_target_for_values, load_data_memory_budgets_for_concurrency,
        load_data_memory_budgets_for_values,
    };

    const MIB: usize = 1024 * 1024;

    #[test]
    fn shadow_sorter_and_pipeline_share_executor_target() {
        let (sorter, pipeline) =
            load_data_memory_budgets_for_values(1, false, 180 * MIB, 600 * MIB);
        assert_eq!(sorter, 420 * MIB);
        assert_eq!(pipeline, 180 * MIB);
        assert_eq!(sorter + pipeline, 600 * MIB);
    }

    #[test]
    fn shadow_sorter_preserves_minimum_when_query_override_is_oversized() {
        let (sorter, pipeline) = load_data_memory_budgets_for_values(2, false, 512 * MIB, 64 * MIB);
        assert_eq!(sorter, 8 * MIB);
        assert_eq!(pipeline, 56 * MIB);
        assert_eq!(sorter + pipeline, 64 * MIB);
    }

    #[test]
    fn live_sorter_keeps_the_existing_query_budget_policy() {
        assert_eq!(
            load_data_memory_budgets_for_values(2, true, 32 * MIB, 64 * MIB),
            (32 * MIB, 32 * MIB)
        );
        assert_eq!(
            load_data_memory_budgets_for_values(2, true, 512 * MIB, 64 * MIB),
            (512 * MIB, 0)
        );
    }

    #[test]
    fn load_without_sorters_can_use_the_query_budget() {
        assert_eq!(
            load_data_memory_budgets_for_values(0, false, 32 * MIB, 64 * MIB),
            (0, 32 * MIB)
        );
    }

    #[test]
    fn concurrent_loads_share_the_executor_envelope() {
        let budgets = (1..=4)
            .map(|active| {
                load_data_memory_budgets_for_concurrency(1, false, 180 * MIB, 600 * MIB, active)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            budgets,
            vec![
                (420 * MIB, 180 * MIB),
                (210 * MIB, 90 * MIB),
                (140 * MIB, 60 * MIB),
                (105 * MIB, 45 * MIB)
            ]
        );
        assert!(budgets
            .iter()
            .all(|(sorter, pipeline)| sorter + pipeline <= 600 * MIB));
    }

    #[test]
    fn minimum_sorter_reclaims_recycled_cache_before_1037() {
        let controlled = 1_676_831_737;
        let requested = 4 * MIB;
        let operational = 1_677_721_600;
        let buffer_raw = 1_258_291_200;
        let headroom = 64 * MIB;
        let minimum_buffer = 16 * MIB;
        let target = load_data_index_reclaim_target_for_values(
            controlled,
            buffer_raw,
            requested,
            headroom,
            minimum_buffer,
            operational,
        )
        .expect("recycled buffer pages can fund the minimum sorter");
        assert_eq!(
            target.saturating_add(requested).saturating_add(headroom),
            operational
        );
    }

    #[test]
    fn minimum_sorter_does_not_reclaim_below_buffer_floor() {
        assert_eq!(
            load_data_index_reclaim_target_for_values(
                95 * MIB,
                16 * MIB,
                8 * MIB,
                4 * MIB,
                16 * MIB,
                100 * MIB,
            ),
            None
        );
    }
}

impl LoadDataIndexBuilders {
    fn lock_keys(
        db: &Database,
        table_id: u64,
        txn_id: u64,
        index: &IndexSchema,
        entries: &[IndexEntry],
    ) -> Result<(), QueryError> {
        if !index.enforces_uniqueness() {
            return Ok(());
        }
        db.lock_unique_keys_nonblocking(
            txn_id,
            entries
                .iter()
                .filter(|entry| {
                    index.primary || !entry.key.0.iter().any(|part| matches!(part, KeyPart::Null))
                })
                .map(|entry| (table_id, index.name.as_str(), entry.key.clone()))
                .collect(),
        )?;
        Ok(())
    }

    /// Consume one row-ordered batch of keys after the heap writer has
    /// assigned its RowIds. For an initially-empty index we append logical
    /// IndexBatch WAL now, before any direct tree page is published. This lets
    /// the final sorted stream rebuild use the last logical LSN without ever
    /// materialising the complete entry list. Existing indexes retain the
    /// normal chunked insert path at finish so pre-existing entries are not
    /// replaced.
    pub(crate) fn push_batch(
        &mut self,
        db: &Database,
        txn_id: u64,
        index_keys: Vec<LoadDataIndexKeyBatch>,
        row_ids: &[RowId],
    ) -> Result<(), QueryError> {
        if self.plans.is_empty() {
            return Ok(());
        }
        if index_keys.len() != self.plans.len() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "LOAD DATA index-key batch does not match table indexes",
            ));
        }
        for (plan, index_keys) in self.plans.iter_mut().zip(index_keys) {
            if let LoadDataIndexKeyBatch::EncodedFixed { key_width, keys } = index_keys {
                if keys.len() != row_ids.len().saturating_mul(key_width) {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "LOAD DATA fixed encoded key batch does not match inserted rows",
                    ));
                }
                let LoadDataIndexSorter::Encoded { program, sorter } = &mut plan.sorter else {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "LOAD DATA fixed encoded batch does not match sorter",
                    ));
                };
                if self.emit_logical_wal
                    && (plan.empty_at_start || plan.index.enforces_uniqueness())
                {
                    let mut entries = Vec::with_capacity(row_ids.len());
                    for (key, row_id) in keys.chunks_exact(key_width).zip(row_ids.iter().copied()) {
                        let pending = EncodedPendingIndexRecord {
                            order_key: smallvec::SmallVec::from_slice(key),
                            materialized_key: None,
                            has_null: program
                                .fixed_ops
                                .as_ref()
                                .is_some_and(|ops| fixed_order_key_has_null(key, ops)),
                        };
                        entries.push(
                            program.into_index_entry(program.attach_row_id(pending, row_id))?,
                        );
                    }
                    Self::lock_keys(db, self.table_id, txn_id, &plan.index, &entries)?;
                    let lsn = if plan.empty_at_start {
                        db.with_index_read(self.table_id, &plan.index.name, |file| {
                            file.append_index_batch_wal(txn_id, WalIndexOpKind::Insert, &entries)
                        })?
                    } else {
                        None
                    };
                    if lsn.is_some() {
                        plan.last_lsn = lsn;
                    }
                }
                sorter.push_fixed_keys_with_row_ids(db.data_dir(), &keys, key_width, row_ids)?;
                continue;
            }
            if let LoadDataIndexKeyBatch::Encoded(per_row) = index_keys {
                if per_row.len() != row_ids.len() {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "LOAD DATA encoded key batch does not match inserted rows",
                    ));
                }
                let LoadDataIndexSorter::Encoded { program, sorter } = &mut plan.sorter else {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "LOAD DATA encoded batch does not match sorter",
                    ));
                };
                let mut wal_entries = (self.emit_logical_wal
                    && (plan.empty_at_start || plan.index.enforces_uniqueness()))
                .then(Vec::<IndexEntry>::new);
                for (records, row_id) in per_row.into_iter().zip(row_ids.iter().copied()) {
                    for record in records {
                        if let Some(entries) = &mut wal_entries {
                            entries.push(
                                program.into_index_entry(
                                    program.attach_row_id(record.clone(), row_id),
                                )?,
                            );
                        }
                        sorter.push(db.data_dir(), record, row_id)?;
                    }
                }
                if let Some(entries) = wal_entries {
                    Self::lock_keys(db, self.table_id, txn_id, &plan.index, &entries)?;
                    if !plan.empty_at_start {
                        continue;
                    }
                    let lsn = db.with_index_read(self.table_id, &plan.index.name, |file| {
                        file.append_index_batch_wal(txn_id, WalIndexOpKind::Insert, &entries)
                    })?;
                    if lsn.is_some() {
                        plan.last_lsn = lsn;
                    }
                }
                continue;
            }
            let LoadDataIndexKeyBatch::Generic(per_row_keys) = index_keys else {
                unreachable!("encoded fixed-stride LOAD batch handled above")
            };
            if per_row_keys.len() != row_ids.len() {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "LOAD DATA index-key batch does not match inserted rows",
                ));
            }
            let key_count = per_row_keys
                .iter()
                .fold(0usize, |sum, keys| sum.saturating_add(keys.len()));

            let mut entries = Vec::with_capacity(key_count);
            for (keys, row_id) in per_row_keys.into_iter().zip(row_ids.iter().copied()) {
                entries.extend(keys.into_iter().map(|key| IndexEntry { key, row_id }));
            }
            if self.emit_logical_wal {
                Self::lock_keys(db, self.table_id, txn_id, &plan.index, &entries)?;
            }
            if self.emit_logical_wal && plan.empty_at_start {
                let lsn = db.with_index_read(self.table_id, &plan.index.name, |file| {
                    file.append_index_batch_wal(txn_id, WalIndexOpKind::Insert, &entries)
                })?;
                if lsn.is_some() {
                    plan.last_lsn = lsn;
                }
            }
            for entry in entries {
                let LoadDataIndexSorter::Generic(sorter) = &mut plan.sorter else {
                    unreachable!("encoded LOAD sorter handled before generic materialization")
                };
                sorter.push(db.data_dir(), entry)?;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(self, db: &Database, txn_id: u64) -> Result<(), QueryError> {
        for plan in self.plans {
            let LoadDataIndexPlan {
                index,
                sorter,
                empty_at_start,
                last_lsn,
            } = plan;
            let sorted = match sorter {
                LoadDataIndexSorter::Encoded { program, sorter } => {
                    LoadDataSortedEntries::Encoded {
                        program,
                        sorted: sorter.finish(db.data_dir())?,
                    }
                }
                LoadDataIndexSorter::Generic(sorter) => {
                    LoadDataSortedEntries::Generic(sorter.finish(db.data_dir())?)
                }
            };
            if empty_at_start {
                Self::finish_empty_plan(db, self.table_id, txn_id, index, last_lsn, sorted)?;
            } else {
                Self::finish_existing_plan(db, self.table_id, txn_id, &index, sorted)?;
            }
        }
        Ok(())
    }

    fn finish_empty_plan(
        db: &Database,
        table_id: u64,
        txn_id: u64,
        index: IndexSchema,
        last_lsn: Option<u64>,
        sorted: LoadDataSortedEntries,
    ) -> Result<(), QueryError> {
        db.with_index_mut(table_id, &index.name, |file| {
            // `empty_at_start` is only a planning observation. Concurrent
            // LOAD DATA statements may all observe the same empty index, then
            // reach publication in a different order. Revalidate while the
            // outer index handle is exclusively held: exactly one finisher
            // may use the destructive bottom-up builder, and every later one
            // merges its already-WAL-logged stream into the published tree.
            if !file.is_empty()? {
                return Self::finish_prelogged_existing_file(
                    file, txn_id, last_lsn, &index, sorted,
                );
            }
            match sorted {
                LoadDataSortedEntries::Encoded { program, sorted } => {
                    bulk_load_prelogged_encoded_sorted(
                        file, txn_id, last_lsn, &index, program, sorted,
                    )
                }
                LoadDataSortedEntries::Generic(SortedIndexEntries::Memory(entries)) => {
                    let stream = entries.into_iter().map(Ok);
                    if index.unique {
                        file.bulk_load_prelogged_sorted_stream(
                            txn_id,
                            last_lsn,
                            UniqueCheckedIndexEntries::new(stream, &index),
                        )
                    } else {
                        file.bulk_load_prelogged_sorted_stream(txn_id, last_lsn, stream)
                    }
                }
                LoadDataSortedEntries::Generic(SortedIndexEntries::Spilled(runs)) => {
                    let stream = runs.into_merger()?;
                    if index.unique {
                        file.bulk_load_prelogged_sorted_stream(
                            txn_id,
                            last_lsn,
                            UniqueCheckedIndexEntries::new(stream, &index),
                        )
                    } else {
                        file.bulk_load_prelogged_sorted_stream(txn_id, last_lsn, stream)
                    }
                }
            }
        })
    }

    fn finish_prelogged_existing_file(
        file: &IndexFile,
        txn_id: u64,
        last_lsn: Option<u64>,
        index: &IndexSchema,
        sorted: LoadDataSortedEntries,
    ) -> Result<(), QueryError> {
        const CHUNK: usize = 4096;
        let mut previous_unique = None;
        let flush = |entries: &mut Vec<(IndexKey, RowId)>| -> Result<(), QueryError> {
            if entries.is_empty() {
                return Ok(());
            }
            file.insert_prelogged_batch_deferred(txn_id, std::mem::take(entries), last_lsn)
        };
        match sorted {
            LoadDataSortedEntries::Encoded { program, sorted } => {
                let mut chunk = Vec::with_capacity(CHUNK);
                for entry in validated_encoded_index_iter(sorted, program, index)? {
                    let entry = entry?;
                    Self::validate_prelogged_unique_entry(
                        file,
                        index,
                        &mut previous_unique,
                        &entry,
                    )?;
                    chunk.push((entry.key, entry.row_id));
                    if chunk.len() == CHUNK {
                        flush(&mut chunk)?;
                    }
                }
                flush(&mut chunk)
            }
            LoadDataSortedEntries::Generic(SortedIndexEntries::Memory(entries)) => {
                let mut chunk = Vec::with_capacity(CHUNK);
                for entry in entries {
                    Self::validate_prelogged_unique_entry(
                        file,
                        index,
                        &mut previous_unique,
                        &entry,
                    )?;
                    chunk.push((entry.key, entry.row_id));
                    if chunk.len() == CHUNK {
                        flush(&mut chunk)?;
                    }
                }
                flush(&mut chunk)
            }
            LoadDataSortedEntries::Generic(SortedIndexEntries::Spilled(runs)) => {
                let mut merger = runs.into_merger()?;
                let mut chunk = Vec::with_capacity(CHUNK);
                while let Some(entry) = merger.next_entry()? {
                    Self::validate_prelogged_unique_entry(
                        file,
                        index,
                        &mut previous_unique,
                        &entry,
                    )?;
                    chunk.push((entry.key, entry.row_id));
                    if chunk.len() == CHUNK {
                        flush(&mut chunk)?;
                    }
                }
                flush(&mut chunk)
            }
        }
    }

    fn validate_prelogged_unique_entry(
        file: &IndexFile,
        index: &IndexSchema,
        previous: &mut Option<IndexKey>,
        entry: &IndexEntry,
    ) -> Result<(), QueryError> {
        if !index.unique {
            return Ok(());
        }
        let has_null = entry.key.0.iter().any(|part| matches!(part, KeyPart::Null));
        if has_null {
            if index.primary {
                return Err(error(
                    crate::error::catalog::ER_INVALID_USE_OF_NULL,
                    "invalid use of NULL value for primary key",
                ));
            }
            *previous = None;
            return Ok(());
        }
        if previous.as_ref() == Some(&entry.key) || file.get_unique_one(&entry.key)?.is_some() {
            return Err(error(
                crate::error::catalog::ER_DUP_ENTRY,
                format!("duplicate entry for key '{}'", index.name),
            ));
        }
        *previous = Some(entry.key.clone());
        Ok(())
    }

    fn finish_existing_plan(
        db: &Database,
        table_id: u64,
        txn_id: u64,
        index: &IndexSchema,
        sorted: LoadDataSortedEntries,
    ) -> Result<(), QueryError> {
        const CHUNK: usize = 4096;
        let mut previous_unique = None;
        let mut flush = |entries: &mut Vec<(IndexKey, RowId)>| -> Result<(), QueryError> {
            if entries.is_empty() {
                return Ok(());
            }
            let owned = std::mem::take(entries);
            db.with_index_mut(table_id, &index.name, |file| {
                if index.unique {
                    for (key, row_id) in &owned {
                        Self::validate_prelogged_unique_entry(
                            file,
                            index,
                            &mut previous_unique,
                            &IndexEntry {
                                key: key.clone(),
                                row_id: *row_id,
                            },
                        )?;
                    }
                }
                file.insert_batch_deferred(txn_id, owned, true)
            })
        };
        match sorted {
            LoadDataSortedEntries::Encoded { program, sorted } => {
                let mut chunk = Vec::with_capacity(CHUNK);
                let validation_index = program.index.clone();
                for entry in validated_encoded_index_iter(sorted, program, &validation_index)? {
                    let entry = entry?;
                    chunk.push((entry.key, entry.row_id));
                    if chunk.len() == CHUNK {
                        flush(&mut chunk)?;
                    }
                }
                flush(&mut chunk)
            }
            LoadDataSortedEntries::Generic(SortedIndexEntries::Memory(entries)) => {
                let mut chunk = Vec::with_capacity(CHUNK);
                for entry in entries {
                    chunk.push((entry.key, entry.row_id));
                    if chunk.len() == CHUNK {
                        flush(&mut chunk)?;
                    }
                }
                flush(&mut chunk)
            }
            LoadDataSortedEntries::Generic(SortedIndexEntries::Spilled(runs)) => {
                let mut merger = runs.into_merger()?;
                let mut chunk = Vec::with_capacity(CHUNK);
                while let Some(entry) = merger.next_entry()? {
                    chunk.push((entry.key, entry.row_id));
                    if chunk.len() == CHUNK {
                        flush(&mut chunk)?;
                    }
                }
                flush(&mut chunk)
            }
        }
    }
}

struct DdlCopyIndexPlan {
    index: IndexSchema,
    buffer: DdlCopyIndexBuffer,
}

enum DdlCopyIndexBuffer {
    Encoded {
        program: Arc<CompiledIndexKeyProgram>,
        sorter: EncodedIndexSorter,
    },
    Generic(IndexEntryExternalSorter),
}

#[derive(Clone)]
enum DdlCopyIndexKeyPlan {
    Encoded(Arc<CompiledIndexKeyProgram>),
    Generic(IndexSchema),
}

enum DdlCopyPreparedKey {
    Encoded(Vec<EncodedPendingIndexRecord>),
    Generic(Vec<IndexKey>),
}

struct DdlCopyPreparedRow {
    source_row: RowId,
    values: Option<Vec<Value>>,
    payload: Vec<u8>,
    keys: smallvec::SmallVec<[DdlCopyPreparedKey; 2]>,
}

fn key_part_payload_capacity(part: &KeyPart) -> usize {
    match part {
        KeyPart::Bytes(value) => value.capacity(),
        KeyPart::String(value) => value.capacity(),
        KeyPart::CollatedString { value, .. } => value.capacity(),
        KeyPart::DecimalWide { value, .. } => value.bits() as usize / 8 + 1,
        _ => 0,
    }
}

#[inline]
fn fixed_scalar_sort_kind(part: &KeyPart) -> Option<u8> {
    Some(match part {
        KeyPart::Int(_) => 1,
        KeyPart::UInt(_) => 2,
        KeyPart::Float32(_) => 3,
        KeyPart::Float64(_) => 4,
        KeyPart::Date(_) => 5,
        KeyPart::Time(_) => 6,
        KeyPart::DateTime(_) => 7,
        KeyPart::Year(_) => 8,
        _ => return None,
    })
}

/// Prove that every non-NULL entry is the same single fixed-width scalar
/// variant. The proof lets the parallel comparison sort bypass generic
/// `IndexKey`/`SmallVec` walking while retaining the existing worker degree and
/// scratch lifetime.
fn fixed_scalar_index_entries_eligible(entries: &[IndexEntry]) -> bool {
    let mut kind = None;
    for entry in entries {
        let [part] = entry.key.0.as_slice() else {
            return false;
        };
        if matches!(part, KeyPart::Null) {
            continue;
        }
        let Some(candidate) = fixed_scalar_sort_kind(part) else {
            return false;
        };
        if kind.is_some_and(|seen| seen != candidate) {
            return false;
        }
        kind = Some(candidate);
    }
    true
}

#[inline]
fn fixed_scalar_index_entry_cmp(left: &IndexEntry, right: &IndexEntry) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let order = match (left.key.0.as_slice(), right.key.0.as_slice()) {
        ([KeyPart::Null], [KeyPart::Null]) => Ordering::Equal,
        ([KeyPart::Null], [_]) => Ordering::Less,
        ([_], [KeyPart::Null]) => Ordering::Greater,
        ([KeyPart::Int(left)], [KeyPart::Int(right)])
        | ([KeyPart::Time(left)], [KeyPart::Time(right)])
        | ([KeyPart::DateTime(left)], [KeyPart::DateTime(right)]) => left.cmp(right),
        ([KeyPart::UInt(left)], [KeyPart::UInt(right)])
        | ([KeyPart::Float64(left)], [KeyPart::Float64(right)]) => left.cmp(right),
        ([KeyPart::Float32(left)], [KeyPart::Float32(right)]) => left.cmp(right),
        ([KeyPart::Date(left)], [KeyPart::Date(right)]) => left.cmp(right),
        ([KeyPart::Year(left)], [KeyPart::Year(right)]) => left.cmp(right),
        _ => return left.cmp(right),
    };
    order.then_with(|| left.row_id.cmp(&right.row_id))
}

fn sort_index_entries(entries: &mut Vec<IndexEntry>, degree: usize) {
    if fixed_scalar_index_entries_eligible(entries) {
        crate::util::par_sort::parallel_sort_unstable_dedup_by(
            entries,
            degree,
            fixed_scalar_index_entry_cmp,
        );
    } else if radix_sort_short_ascii_index_entries(entries) {
        entries.dedup();
    } else {
        crate::util::par_sort::parallel_sort_unstable_dedup(entries, degree);
    }
}

impl DdlCopyPreparedRow {
    fn capacity_bytes(&self) -> usize {
        let value_bytes = self.values.as_ref().map_or(0, |v| {
            crate::engine::executor::mem_budget::estimate_row_bytes(v)
        });
        let mut bytes = self
            .payload
            .capacity()
            .saturating_add(value_bytes)
            .saturating_add(if self.keys.spilled() {
                self.keys
                    .capacity()
                    .saturating_mul(std::mem::size_of::<DdlCopyPreparedKey>())
            } else {
                0
            });
        for key in &self.keys {
            match key {
                DdlCopyPreparedKey::Encoded(records) => {
                    bytes = bytes.saturating_add(
                        records
                            .capacity()
                            .saturating_mul(std::mem::size_of::<EncodedPendingIndexRecord>()),
                    );
                    for record in records {
                        bytes = bytes
                            .saturating_add(record.order_key.capacity())
                            .saturating_add(
                                record.materialized_key.as_ref().map_or(0, Vec::capacity),
                            );
                    }
                }
                DdlCopyPreparedKey::Generic(keys) => {
                    bytes = bytes.saturating_add(
                        keys.capacity()
                            .saturating_mul(std::mem::size_of::<IndexKey>()),
                    );
                    for key in keys {
                        if key.0.spilled() {
                            bytes = bytes.saturating_add(
                                key.0
                                    .capacity()
                                    .saturating_mul(std::mem::size_of::<KeyPart>()),
                            );
                        }
                        for part in &key.0 {
                            bytes = bytes.saturating_add(key_part_payload_capacity(part));
                        }
                    }
                }
            }
        }
        bytes
    }
}

fn radix_sort_short_ascii_index_entries(entries: &mut [IndexEntry]) -> bool {
    if entries.len() < 2 {
        return true;
    }
    let arity = entries[0].key.0.len();
    if !(1..=4).contains(&arity) || entries.iter().any(|entry| entry.key.0.len() != arity) {
        return false;
    }
    let mut collations = Vec::with_capacity(arity);
    for position in 0..arity {
        let mut collation_id = None;
        let mut plain_string = false;
        for entry in entries.iter() {
            match &entry.key.0[position] {
                KeyPart::Null => continue,
                KeyPart::CollatedString {
                    collation_id: id,
                    value,
                } => {
                    if plain_string
                        || collation_id.replace(*id).is_some_and(|seen| seen != *id)
                        || !value.is_ascii()
                        || value.len() > 64
                    {
                        return false;
                    }
                }
                KeyPart::String(value) => {
                    if collation_id.is_some() || !value.is_ascii() || value.len() > 64 {
                        return false;
                    }
                    plain_string = true;
                }
                _ => return false,
            }
        }
        if plain_string {
            collations.push(Some((true, false)));
            continue;
        }
        let Some(id) = collation_id else {
            // An all-NULL part carries no collation identity; comparison is
            // already equal for the whole part, so use a benign placeholder.
            collations.push(None);
            continue;
        };
        let Some(info) = crate::charset::resolve_collation_id(id) else {
            return false;
        };
        if entries.iter().any(|entry| match &entry.key.0[position] {
            KeyPart::CollatedString { value, .. } => {
                (info.pad_space && value.ends_with(' '))
                    || (info.uca_0900 && !info.binary && value.as_bytes().contains(&0))
            }
            _ => false,
        }) {
            return false;
        }
        collations.push(Some((info.case_sensitive, info.pad_space)));
    }

    #[derive(Clone, Copy)]
    enum State {
        Part(usize),
        Byte(usize, usize),
    }
    let bucket = |entry: &IndexEntry, state: State| -> usize {
        match state {
            State::Part(part) => usize::from(!matches!(entry.key.0[part], KeyPart::Null)),
            State::Byte(part, position) => {
                let value = match &entry.key.0[part] {
                    KeyPart::CollatedString { value, .. } | KeyPart::String(value) => value,
                    _ => unreachable!("string-byte state only receives string values"),
                };
                let (case_sensitive, _) = collations[part].expect("non-NULL part has collation");
                value
                    .as_bytes()
                    .get(position)
                    .map(|byte| {
                        usize::from(if case_sensitive {
                            *byte
                        } else {
                            byte.to_ascii_lowercase()
                        }) + 1
                    })
                    .unwrap_or(0)
            }
        }
    };
    let mut pending = vec![(0usize, entries.len(), State::Part(0))];
    while let Some((lo, hi, state)) = pending.pop() {
        if hi - lo < 2 {
            continue;
        }
        let mut counts = [0usize; 257];
        for entry in &entries[lo..hi] {
            counts[bucket(entry, state)] += 1;
        }
        let mut starts = [0usize; 257];
        let mut cursor = lo;
        for (idx, count) in counts.iter().copied().enumerate() {
            starts[idx] = cursor;
            cursor += count;
        }
        let mut next = starts;
        for expected in 0..257 {
            let end = starts[expected] + counts[expected];
            while next[expected] < end {
                let actual = bucket(&entries[next[expected]], state);
                if actual == expected {
                    next[expected] += 1;
                } else {
                    entries.swap(next[expected], next[actual]);
                    next[actual] += 1;
                }
            }
        }
        let mut schedule_next_part = |pending: &mut Vec<(usize, usize, State)>, lo, hi, part| {
            if hi - lo < 2 {
                return;
            }
            if part + 1 == arity {
                entries[lo..hi].sort_unstable_by_key(|entry| entry.row_id);
            } else {
                pending.push((lo, hi, State::Part(part + 1)));
            }
        };
        match state {
            State::Part(part) => {
                schedule_next_part(&mut pending, starts[0], starts[0] + counts[0], part);
                if counts[1] > 1 {
                    pending.push((starts[1], starts[1] + counts[1], State::Byte(part, 0)));
                }
            }
            State::Byte(part, position) => {
                schedule_next_part(&mut pending, starts[0], starts[0] + counts[0], part);
                for idx in (1..257).rev() {
                    if counts[idx] > 1 {
                        pending.push((
                            starts[idx],
                            starts[idx] + counts[idx],
                            State::Byte(part, position + 1),
                        ));
                    }
                }
            }
        }
    }
    true
}

#[test]
fn fixed_scalar_comparator_matches_generic_index_entry_order() {
    let row_ids = [
        RowId::new(9, 2),
        RowId::new(1, 7),
        RowId::new(1, 1),
        RowId::new(u32::MAX, u16::MAX),
    ];
    let cases = vec![
        vec![
            KeyPart::Int(7),
            KeyPart::Int(-9),
            KeyPart::Null,
            KeyPart::Int(7),
        ],
        vec![
            KeyPart::UInt(7),
            KeyPart::UInt(0),
            KeyPart::Null,
            KeyPart::UInt(u64::MAX),
        ],
        vec![
            KeyPart::Float32(7),
            KeyPart::Float32(0),
            KeyPart::Null,
            KeyPart::Float32(u32::MAX),
        ],
        vec![
            KeyPart::Float64(7),
            KeyPart::Float64(0),
            KeyPart::Null,
            KeyPart::Float64(u64::MAX),
        ],
        vec![
            KeyPart::Date(7),
            KeyPart::Date(-9),
            KeyPart::Null,
            KeyPart::Date(i32::MAX),
        ],
        vec![
            KeyPart::Time(7),
            KeyPart::Time(-9),
            KeyPart::Null,
            KeyPart::Time(i64::MAX),
        ],
        vec![
            KeyPart::DateTime(7),
            KeyPart::DateTime(-9),
            KeyPart::Null,
            KeyPart::DateTime(i64::MAX),
        ],
        vec![
            KeyPart::Year(7),
            KeyPart::Year(0),
            KeyPart::Null,
            KeyPart::Year(u8::MAX),
        ],
    ];
    for parts in cases {
        let actual = parts
            .into_iter()
            .zip(row_ids)
            .map(|(part, row_id)| IndexEntry {
                key: IndexKey::from(vec![part]),
                row_id,
            })
            .collect::<Vec<_>>();
        assert!(fixed_scalar_index_entries_eligible(&actual));
        for left in &actual {
            for right in &actual {
                assert_eq!(fixed_scalar_index_entry_cmp(left, right), left.cmp(right));
            }
        }
    }
}

#[test]
fn fixed_scalar_comparator_gate_rejects_mixed_and_composite_keys() {
    let mut mixed = vec![
        IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(1)]),
            row_id: RowId::new(1, 0),
        },
        IndexEntry {
            key: IndexKey::from(vec![KeyPart::UInt(1)]),
            row_id: RowId::new(2, 0),
        },
    ];
    assert!(!fixed_scalar_index_entries_eligible(&mixed));
    mixed[1].key = IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(2)]);
    assert!(!fixed_scalar_index_entries_eligible(&mixed));
}

#[test]
fn offline_multi_collated_radix_matches_index_key_order() {
    let id = crate::charset::collation_id("utf8mb4_0900_ai_ci");
    let values = [
        (Some("M"), Some("Single"), Some("College")),
        (Some("f"), Some("married"), Some("High School")),
        (None, Some("single"), Some("Primary")),
        (Some("F"), None, Some("Advanced")),
        (Some("m"), Some("single"), None),
    ];
    let mut entries = values
        .iter()
        .enumerate()
        .rev()
        .map(|(idx, values)| IndexEntry {
            key: IndexKey::from(
                [values.0, values.1, values.2]
                    .into_iter()
                    .map(|value| {
                        value.map_or(KeyPart::Null, |value| KeyPart::CollatedString {
                            collation_id: id,
                            value: value.to_string(),
                        })
                    })
                    .collect::<Vec<_>>(),
            ),
            row_id: RowId {
                page_id: idx as u32,
                slot_id: 0,
            },
        })
        .collect::<Vec<_>>();
    let mut expected = entries.clone();
    expected.sort_unstable();
    assert!(radix_sort_short_ascii_index_entries(&mut entries));
    assert_eq!(entries, expected);

    let mut plain = entries
        .iter()
        .cloned()
        .map(|mut entry| {
            for part in &mut entry.key.0 {
                if let KeyPart::CollatedString { value, .. } = part {
                    *part = KeyPart::String(value.clone());
                }
            }
            entry
        })
        .collect::<Vec<_>>();
    plain.reverse();
    let mut plain_expected = plain.clone();
    plain_expected.sort_unstable();
    assert!(radix_sort_short_ascii_index_entries(&mut plain));
    assert_eq!(plain, plain_expected);
}

/// RowIds, eliminating one full shadow-heap scan per index.
struct DdlCopyIndexBuilders {
    plans: Vec<DdlCopyIndexPlan>,
    _charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

// Nonblocking shared admission: concurrent partition DDLs cannot each
// multiply their index-finish workers by the configured parallel degree.
static PARTITION_INDEX_FINISH_WORKERS: AtomicUsize = AtomicUsize::new(0);
struct PartitionIndexFinishWorkers(usize);
impl PartitionIndexFinishWorkers {
    fn acquire(requested: usize) -> Self {
        let capacity = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .saturating_sub(1);
        let mut active = PARTITION_INDEX_FINISH_WORKERS.load(Ordering::Acquire);
        loop {
            let grant = requested.min(capacity.saturating_sub(active));
            if grant == 0 {
                return Self(0);
            }
            match PARTITION_INDEX_FINISH_WORKERS.compare_exchange_weak(
                active,
                active + grant,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Self(grant),
                Err(now) => active = now,
            }
        }
    }
}
impl Drop for PartitionIndexFinishWorkers {
    fn drop(&mut self) {
        PARTITION_INDEX_FINISH_WORKERS.fetch_sub(self.0, Ordering::AcqRel);
    }
}

#[cfg(test)]
impl Database {
    pub(crate) fn partition_index_finish_workers_for_test(&self) -> usize {
        PARTITION_INDEX_FINISH_WORKERS.load(Ordering::Acquire)
    }
}

impl DdlCopyIndexBuilders {
    fn new(table: &TableSchema) -> Result<Self, QueryError> {
        if table.indexes.is_empty() {
            return Ok(Self {
                plans: Vec::new(),
                _charge: None,
            });
        }
        let minimum = table.indexes.len().saturating_mul(1024 * 1024);
        let mut aggregate_budget =
            crate::engine::executor::mem_budget::query_mem_limit_bytes().max(minimum);
        let charge = loop {
            match crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DdlCopy,
                aggregate_budget,
            ) {
                Ok(charge) => break charge,
                Err(_) if aggregate_budget > minimum => {
                    aggregate_budget = (aggregate_budget / 2).max(minimum);
                }
                Err(err) => return Err(err),
            }
        };
        let per_sorter = (aggregate_budget / table.indexes.len()).max(1024 * 1024);
        let plans = table
            .indexes
            .iter()
            .cloned()
            .map(|index| {
                let buffer = if encoded_btree_build_eligible(&index) {
                    let program = Arc::new(CompiledIndexKeyProgram::compile(table, &index)?);
                    DdlCopyIndexBuffer::Encoded {
                        program: program.clone(),
                        sorter: EncodedIndexSorter::new(program, per_sorter),
                    }
                } else {
                    DdlCopyIndexBuffer::Generic(IndexEntryExternalSorter::with_exact_run_bytes(
                        per_sorter,
                    ))
                };
                Ok(DdlCopyIndexPlan { index, buffer })
            })
            .collect::<Result<Vec<_>, QueryError>>()?;
        Ok(Self {
            plans,
            _charge: Some(charge),
        })
    }

    fn key_plans(&self, _table: &TableSchema) -> Vec<DdlCopyIndexKeyPlan> {
        self.plans
            .iter()
            .map(|plan| match &plan.buffer {
                DdlCopyIndexBuffer::Encoded { program, .. } => {
                    DdlCopyIndexKeyPlan::Encoded(program.clone())
                }
                DdlCopyIndexBuffer::Generic(_) => DdlCopyIndexKeyPlan::Generic(plan.index.clone()),
            })
            .collect()
    }
    fn prepare_keys(
        table: &TableSchema,
        plans: &[DdlCopyIndexKeyPlan],
        values: &mut [Value],
    ) -> Result<smallvec::SmallVec<[DdlCopyPreparedKey; 2]>, QueryError> {
        plans
            .iter()
            .map(|plan| match plan {
                DdlCopyIndexKeyPlan::Encoded(program) => Ok(DdlCopyPreparedKey::Encoded(
                    program.encode_row(table, values)?,
                )),
                DdlCopyIndexKeyPlan::Generic(index) => Ok(DdlCopyPreparedKey::Generic(
                    table.index_keys(index, values)?,
                )),
            })
            .collect()
    }
    fn push_prepared_batch(
        &mut self,
        data_dir: &Path,
        rows: Vec<DdlCopyPreparedRow>,
        row_ids: &[RowId],
    ) -> Result<(), QueryError> {
        if rows.len() != row_ids.len() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "ALTER COPY index fan-out row-id length mismatch",
            ));
        }
        for (row, row_id) in rows.into_iter().zip(row_ids.iter().copied()) {
            if row.keys.len() != self.plans.len() {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "ALTER COPY prepared index-key length mismatch",
                ));
            }
            for (plan, key) in self.plans.iter_mut().zip(row.keys) {
                match (&mut plan.buffer, key) {
                    (
                        DdlCopyIndexBuffer::Encoded { sorter, .. },
                        DdlCopyPreparedKey::Encoded(records),
                    ) => {
                        for record in records {
                            sorter.push(data_dir, record, row_id)?;
                        }
                    }
                    (DdlCopyIndexBuffer::Generic(sorter), DdlCopyPreparedKey::Generic(keys)) => {
                        for key in keys {
                            sorter.push(data_dir, IndexEntry { key, row_id })?;
                        }
                    }
                    _ => {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "ALTER COPY prepared index-key plan mismatch",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn finish_plan(
        db: &Database,
        table: &TableSchema,
        plan: DdlCopyIndexPlan,
        publish_cache: bool,
        sort_degree: Option<usize>,
    ) -> Result<Option<IndexFile>, QueryError> {
        if publish_cache {
            db.remove_index_cache_entry(table.id, &plan.index.name)?;
        }
        let path = db.index_path_for_table_schema(table, table.id, &plan.index.name);
        db.buffer_pool
            .discard_file(&Arc::new(BufferPool::index_file_key(path.clone())))?;
        let file = match plan.buffer {
            DdlCopyIndexBuffer::Encoded { program, sorter } => {
                let sorter = if let Some(degree) = sort_degree {
                    sorter.with_sort_degree(degree)
                } else {
                    sorter
                };
                let sorted = sorter.finish(db.data_dir())?;
                create_rebuilt_from_encoded_sorted(
                    db,
                    path,
                    &plan.index,
                    program,
                    sorted,
                    &mut None,
                )?
            }
            DdlCopyIndexBuffer::Generic(sorter) => match sorter.finish_with_degree(
                db.data_dir(),
                sort_degree.unwrap_or_else(index_sort_parallel_degree),
            )? {
                SortedIndexEntries::Memory(entries) => {
                    let stream = entries.into_iter().map(Ok);
                    if plan.index.unique {
                        IndexFile::create_rebuilt_from_sorted_stream_with_durability(
                            path,
                            UniqueCheckedIndexEntries::new(stream, &plan.index),
                            db.log.clone(),
                            db.config.durability,
                            db.buffer_pool.clone(),
                        )?
                    } else {
                        IndexFile::create_rebuilt_from_sorted_stream_with_durability(
                            path,
                            stream,
                            db.log.clone(),
                            db.config.durability,
                            db.buffer_pool.clone(),
                        )?
                    }
                }
                SortedIndexEntries::Spilled(runs) => {
                    let stream = runs.into_merger()?;
                    if plan.index.unique {
                        IndexFile::create_rebuilt_from_sorted_stream_with_durability(
                            path,
                            UniqueCheckedIndexEntries::new(stream, &plan.index),
                            db.log.clone(),
                            db.config.durability,
                            db.buffer_pool.clone(),
                        )?
                    } else {
                        IndexFile::create_rebuilt_from_sorted_stream_with_durability(
                            path,
                            stream,
                            db.log.clone(),
                            db.config.durability,
                            db.buffer_pool.clone(),
                        )?
                    }
                }
            },
        };
        file.flush()?;
        if publish_cache {
            db.index_cache.write().insert(
                (table.id, normalize_name(&plan.index.name)),
                Arc::new(parking_lot::RwLock::new(file)),
            );
            Ok(None)
        } else {
            Ok(Some(file))
        }
    }

    fn finish_private(
        self,
        db: &Database,
        table: &TableSchema,
    ) -> Result<Vec<(String, IndexFile)>, QueryError> {
        let DdlCopyIndexBuilders { plans, _charge } = self;
        let mut files = Vec::with_capacity(plans.len());
        for plan in plans {
            let name = plan.index.name.clone();
            let file = Self::finish_plan(db, table, plan, false, None)?
                .expect("private build returns its handle");
            files.push((name, file));
        }
        Ok(files)
    }

    fn finish_partition(self, db: &Database, table: &TableSchema) -> Result<(), QueryError> {
        let DdlCopyIndexBuilders { plans, _charge } = self;
        let degree = index_sort_parallel_degree().max(1);
        let requested = if plans.len() <= 1 {
            degree
        } else {
            degree.min(plans.len())
        };
        let workers = PartitionIndexFinishWorkers::acquire(requested.saturating_sub(1));
        if plans.len() <= 1 || workers.0 == 0 {
            for plan in plans {
                Self::finish_plan(db, table, plan, true, Some(workers.0 + 1))?;
            }
            return Ok(());
        }
        // All sorter capacity remains covered by the single aggregate
        // reservation above. Independent private files can fsync in parallel;
        // inner sorts run serially to avoid nested oversubscription.
        let queue = Mutex::new(plans.into_iter());
        let failure = Mutex::new(None);
        let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
        let run = || loop {
            if failure.lock().unwrap_or_else(|p| p.into_inner()).is_some() {
                break;
            }
            let Some(plan) = queue.lock().unwrap_or_else(|p| p.into_inner()).next() else {
                break;
            };
            let result = crate::engine::cancel::check_cancelled()
                .and_then(|_| Self::finish_plan(db, table, plan, true, Some(1)));
            if let Err(err) = result {
                failure
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get_or_insert(err);
                break;
            }
        };
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..workers.0 {
                let run = &run;
                let cancellation = cancellation.clone();
                if let Ok(handle) = std::thread::Builder::new()
                    .name("mydb-part-index".into())
                    .spawn_scoped(scope, move || {
                        let _cancel = cancellation.enter();
                        run();
                    })
                {
                    handles.push(handle);
                }
            }
            run();
            for handle in handles {
                if handle.join().is_err() {
                    failure
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .get_or_insert_with(|| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "partition index builder panicked",
                            )
                        });
                }
            }
        });
        match failure.into_inner().unwrap_or_else(|p| p.into_inner()) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn finish(self, db: &Database, table: &TableSchema) -> Result<(), QueryError> {
        let DdlCopyIndexBuilders { plans, _charge } = self;
        for plan in plans {
            Self::finish_plan(db, table, plan, true, None)?;
        }
        Ok(())
    }
}

fn estimate_index_entry_sort_bytes(entry: &IndexEntry) -> usize {
    let mut bytes = std::mem::size_of::<IndexEntry>();
    if entry.key.0.spilled() {
        bytes = bytes.saturating_add(
            entry
                .key
                .0
                .capacity()
                .saturating_mul(std::mem::size_of::<KeyPart>()),
        );
    }
    for part in &entry.key.0 {
        bytes = bytes.saturating_add(key_part_payload_capacity(part));
    }
    bytes
}

/// Worker count for the in-memory sort phase of index builds. Mirrors
/// `crate::planner::default_parallel_degree` so users can tune both
/// query-time and DDL-time parallelism through the single
/// `MYDB_PARALLEL_DEGREE` knob, with a `MYDB_PARALLEL_DISABLE=1`
/// kill-switch matching the parallel-query subsystem.
/// Process environment is fixed after startup, but each lookup takes the
/// global environment lock; resolve it once outside tests (which toggle it).
fn index_sort_parallel_degree() -> usize {
    #[cfg(not(test))]
    {
        static DEGREE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *DEGREE.get_or_init(read_index_sort_parallel_degree)
    }
    #[cfg(test)]
    read_index_sort_parallel_degree()
}

fn read_index_sort_parallel_degree() -> usize {
    if env::var("MYDB_PARALLEL_DISABLE")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
        .unwrap_or(false)
    {
        return 1;
    }
    env::var("MYDB_PARALLEL_DEGREE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(2)
}

impl Database {
    pub fn auto_increment_next_value(
        &self,
        table: &TableSchema,
    ) -> Result<Option<u64>, QueryError> {
        if table.auto_increment_column_index().is_none() {
            return Ok(None);
        }
        let max_value = if let Some(max_value) = self.auto_increment.cached_max(table.id) {
            max_value
        } else {
            self.read_auto_increment_max(table.id)?
                .unwrap_or(0)
                .max(self.current_auto_increment_max(table)?)
        };
        Ok(Some(max_value.checked_add(1).ok_or_else(|| {
            error(
                crate::error::catalog::ER_AUTOINC_READ_FAILED,
                "auto-increment value is out of range",
            )
        })?))
    }

    fn insert_indexes_deferred(
        &self,
        txn_id: u64,
        table: &TableSchema,
        values: &[Value],
        row_id: RowId,
    ) -> Result<(), QueryError> {
        self.insert_indexes_deferred_at(txn_id, table.id, table, values, row_id)
    }

    /// Partition-aware rollback variant of [`Self::insert_indexes_deferred`].
    /// Undo records are keyed by the physical heap `storage_id`, so index
    /// compensation must target the indexes beside that heap rather than the
    /// partitioned table's logical `table.id`.
    fn insert_indexes_deferred_at(
        &self,
        txn_id: u64,
        storage_id: u64,
        table: &TableSchema,
        values: &[Value],
        row_id: RowId,
    ) -> Result<(), QueryError> {
        for index in &table.indexes {
            for key in table.index_keys(index, values)? {
                self.with_index_dml(storage_id, &index.name, |index_file| {
                    index_file.insert_deferred(txn_id, key, row_id)
                })?;
            }
        }
        Ok(())
    }

    fn register_index_repair_entries(
        &self,
        txn_id: u64,
        repairs: impl IntoIterator<Item = (IndexRepairKey, RowId, bool)>,
    ) -> Result<(), QueryError> {
        // Validate the active transaction before the registry lease. Never
        // acquire the transaction directory while holding this lease.
        let head = self.undo_head_for_statement(txn_id)?;
        self.index_repair_state.register(
            txn_id,
            repairs,
            &head,
            &self.index_repair_epochs,
            &self.index_repair_version,
        )
    }

    fn register_changed_index_repair_keys(
        &self,
        txn_id: u64,
        table: &TableSchema,
        old_values: &[Value],
        new_values: &[Value],
        row_id: RowId,
        changed_columns: Option<&[usize]>,
        affected_index_positions: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        let mut repairs: smallvec::SmallVec<[(IndexRepairKey, bool); 4]> =
            smallvec::SmallVec::new();
        for (index_pos, index) in table.indexes.iter().enumerate() {
            if !affected_index_positions.map_or_else(
                || index_may_be_affected_by_columns(table, index, changed_columns),
                |positions| positions.binary_search(&index_pos).is_ok(),
            ) {
                continue;
            }
            if index.is_fulltext() || index.has_multi_valued_parts() {
                // Token/MVI-set diff: only disappearing keys need stale
                // visibility repair.
                let new_keys: std::collections::HashSet<IndexKey> =
                    table.index_keys(index, new_values)?.into_iter().collect();
                for old_key in table.index_keys(index, old_values)? {
                    if !new_keys.contains(&old_key) {
                        repairs.push((
                            IndexRepairKey {
                                table_id: table.id,
                                index_name: normalize_name(&index.name),
                                key: old_key,
                            },
                            false,
                        ));
                    }
                }
                continue;
            }
            let old_key = table.index_key(index, old_values)?;
            let new_key = table.index_key(index, new_values)?;
            if old_key.same_stored_value(&new_key) {
                continue;
            }
            repairs.push((
                IndexRepairKey {
                    table_id: table.id,
                    index_name: normalize_name(&index.name),
                    key: old_key,
                },
                true,
            ));
        }
        if !repairs.is_empty() {
            self.register_index_repair_entries(
                txn_id,
                repairs
                    .into_iter()
                    .map(|(key, single_key)| (key, row_id, single_key)),
            )?;
        }
        Ok(())
    }

    fn write_heap_rows(
        &self,
        txn_id: u64,
        table_id: u64,
        writes: &[HeapWrite],
    ) -> Result<(), QueryError> {
        if writes.is_empty() {
            return Ok(());
        }
        // Whole-row WAL is eager. Publish older deferred logical patches and
        // their undo twins first, preserving mutation order for crash undo.
        self.publish_wal_before_eager_heap_write(txn_id)?;
        self.with_heap_mut(table_id, |heap| {
            for write in writes {
                heap.write_mvcc_ext(
                    txn_id,
                    write.row_id,
                    write.header,
                    &write.values,
                    &write.external,
                )?;
            }
            Ok(())
        })
    }

    fn insert_index_batches_deferred(
        &self,
        txn_id: u64,
        table: &TableSchema,
        index_entries: BTreeMap<String, Vec<(IndexKey, RowId)>>,
    ) -> Result<(), QueryError> {
        self.insert_index_batches_deferred_at(txn_id, table.id, table, index_entries)
    }

    /// Partition-aware variant of [`Self::insert_index_batches_deferred`].
    /// Writes to the local index files at `storage_id` (each partition
    /// of a partitioned table has its own per-storage_id index file).
    /// For an unpartitioned table the caller passes `table.id` so this
    /// collapses to the unparameterised path.
    pub(crate) fn insert_index_batches_deferred_at(
        &self,
        txn_id: u64,
        storage_id: u64,
        table: &TableSchema,
        mut index_entries: BTreeMap<String, Vec<(IndexKey, RowId)>>,
    ) -> Result<(), QueryError> {
        for index in &table.indexes {
            let entries = index_entries.remove(&index.name).unwrap_or_default();
            self.insert_index_batch_deferred_at(txn_id, storage_id, index, entries)?;
        }
        Ok(())
    }

    /// Build and apply INSERT entries one index at a time.
    ///
    /// The storage writer can only mutate one B-tree at a time. Retaining one
    /// entry vector per index therefore multiplies the transient working set
    /// without exposing any extra parallelism. This primitive bounds the live
    /// entry capacity to one prepared row batch and releases it before the
    /// next index is materialized.
    pub(crate) fn insert_index_batches_deferred_for_rows(
        &self,
        txn_id: u64,
        table: &TableSchema,
        rows: &[Vec<Value>],
        row_ids: &[RowId],
    ) -> Result<(), QueryError> {
        debug_assert_eq!(rows.len(), row_ids.len());
        if table.indexes.is_empty() {
            return Ok(());
        }

        // Concurrent bulk writers otherwise march through schema order in
        // lockstep: all contend on PRIMARY, then all move to secondary #1,
        // while the other independent B-trees sit idle. Only one index handle
        // is held at a time, so rotating the starting point cannot introduce a
        // cross-index lock cycle. Unique probes and locks were already taken
        // in schema order, preserving duplicate-error precedence.
        let index_start = (txn_id as usize) % table.indexes.len();
        let parallel_degree = index_sort_parallel_degree();
        let simple_scalar_indexes = table.indexes.iter().all(|index| {
            !index.is_fulltext()
                && !index.has_multi_valued_parts()
                && !index.is_spatial()
                && !index.is_vector()
                && index.columns.len() <= 4
                && index.key_expressions.iter().all(Option::is_none)
        });
        let parallel_candidate = rows.len() >= 64
            && table.indexes.len() > 1
            && table.indexes.len() <= parallel_degree
            && simple_scalar_indexes;
        if parallel_candidate {
            // Parallel index fan-out owns one top-level entry Vec per index,
            // instead of the scalar path's single live Vec. Reserve every
            // vector by capacity plus a conservative upper bound for cloned
            // variable-width key payloads. Worker topology is derived only
            // from SQL cardinality/schema/configured degree; insufficient
            // StorageScratch returns HY001 rather than changing topology from
            // live memory state or crossing MYDB_MEMORY_TARGET.
            let value_payload_capacity = |value: &Value| match value {
                Value::Decimal(value) | Value::String(value) => value.capacity(),
                Value::SmallString(value) if value.spilled() => value.capacity(),
                Value::DecimalWide(_) => value.decimal_heap_bytes(),
                Value::Bytes(value) | Value::Json(value) | Value::Geometry(value) => {
                    value.capacity()
                }
                Value::Vector(value) => value.capacity(),
                _ => 0,
            };
            let entry_capacity = table
                .indexes
                .len()
                .saturating_mul(rows.len())
                .saturating_mul(std::mem::size_of::<(IndexKey, RowId)>());
            let payload_capacity = table.indexes.iter().fold(0usize, |total, index| {
                let per_index = rows.iter().fold(0usize, |row_total, row| {
                    let row_payload = index.columns.iter().fold(0usize, |column_total, name| {
                        let payload = table
                            .column_index(name)
                            .ok()
                            .and_then(|column| row.get(column))
                            .map(value_payload_capacity)
                            .unwrap_or(0);
                        // Collation weights/encoded decimal keys can exceed
                        // the source payload. Eightfold plus one small record
                        // allowance is conservative for admitted scalar keys.
                        column_total
                            .saturating_add(payload.saturating_mul(8))
                            .saturating_add(64)
                    });
                    row_total.saturating_add(row_payload)
                });
                total.saturating_add(per_index)
            });
            let parallel_bytes = entry_capacity.saturating_add(payload_capacity);
            let _parallel_charge =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    parallel_bytes,
                )?;
            return std::thread::scope(|scope| {
                let mut workers = Vec::with_capacity(table.indexes.len());
                for index_offset in 0..table.indexes.len() {
                    let index = &table.indexes[(index_start + index_offset) % table.indexes.len()];
                    workers.push(scope.spawn(move || -> Result<(), QueryError> {
                        let mut entries = Vec::with_capacity(rows.len());
                        for (values, row_id) in rows.iter().zip(row_ids.iter().copied()) {
                            entries.push((table.index_key(index, values)?, row_id));
                        }
                        self.insert_index_batch_deferred_at(txn_id, table.id, index, entries)
                    }));
                }
                for worker in workers {
                    worker.join().map_err(|_| {
                        error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "parallel index insert worker panicked",
                        )
                    })??;
                }
                Ok(())
            });
        }
        for index_offset in 0..table.indexes.len() {
            let index = &table.indexes[(index_start + index_offset) % table.indexes.len()];
            let mut entries = Vec::with_capacity(rows.len());
            if index.is_fulltext() || index.has_multi_valued_parts() {
                for (values, row_id) in rows.iter().zip(row_ids.iter().copied()) {
                    for key in table.index_keys(index, values)? {
                        entries.push((key, row_id));
                    }
                }
            } else {
                // Ordinary scalar indexes produce exactly one key per row.
                // Calling `index_keys` here wrapped that key in a fresh Vec
                // and immediately drained it — one allocator round trip per
                // row on the hottest INSERT path.  Keep the plural API for
                // fulltext/multi-valued indexes and build the scalar entry in
                // place.
                for (values, row_id) in rows.iter().zip(row_ids.iter().copied()) {
                    entries.push((table.index_key(index, values)?, row_id));
                }
            }
            self.insert_index_batch_deferred_at(txn_id, table.id, index, entries)?;
        }
        Ok(())
    }

    /// Sort and apply one index's INSERT entries.
    ///
    /// Kept separate because DDL callers may already own pre-built entry
    /// vectors while DML callers use `insert_index_batches_deferred_for_rows`.
    pub(crate) fn insert_index_batch_deferred_at(
        &self,
        txn_id: u64,
        storage_id: u64,
        index: &crate::storage::schema::IndexSchema,
        mut entries: Vec<(IndexKey, RowId)>,
    ) -> Result<(), QueryError> {
        // AUTO_INCREMENT/sequence loads and many replication streams already
        // arrive in strict index order.  Proving that with one linear pass is
        // cheaper than an unconditional O(N log N) sort; equal or descending
        // neighbours fall back to the canonical sort+dedup path, preserving
        // duplicate handling for arbitrary batches.
        if !entries.windows(2).all(|pair| pair[0] < pair[1]) {
            crate::util::par_sort::parallel_sort_unstable_dedup(
                &mut entries,
                index_sort_parallel_degree(),
            );
        }
        self.with_index_dml(storage_id, &index.name, |index_file| {
            // Shared (read) handle lock: concurrent DML / SELECT may be
            // navigating this index, so the destructive empty-tree bulk-load
            // reset is unsafe here. Build incrementally.
            index_file.insert_batch_deferred(txn_id, entries, false)
        })
    }

    /// Updates the secondary indexes touched by `changed_columns`, operating
    /// on the index files at `storage_id` (the partition's physical storage
    /// id). The repair-key registry is bound to the logical `table.id` since
    /// repair operates table-wide.
    pub(crate) fn update_changed_indexes_deferred_at(
        &self,
        txn_id: u64,
        storage_id: u64,
        table: &TableSchema,
        old_values: &[Value],
        new_values: &[Value],
        row_id: RowId,
        changed_columns: Option<&[usize]>,
        affected_index_positions: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        self.register_changed_index_repair_keys(
            txn_id,
            table,
            old_values,
            new_values,
            row_id,
            changed_columns,
            affected_index_positions,
        )?;
        for (index_pos, index) in table.indexes.iter().enumerate() {
            if !affected_index_positions.map_or_else(
                || index_may_be_affected_by_columns(table, index, changed_columns),
                |positions| positions.binary_search(&index_pos).is_ok(),
            ) {
                continue;
            }
            if index.is_fulltext() || index.has_multi_valued_parts() {
                // Differential maintenance: delete keys in old∖new, insert
                // keys in new∖old. A full delete-all + insert-all would
                // churn the B-tree and defeat its exact-duplicate no-op
                // dedup for unchanged keys.
                let old_keys = table.index_keys(index, old_values)?;
                let new_keys = table.index_keys(index, new_values)?;
                let old_set: std::collections::HashSet<&IndexKey> = old_keys.iter().collect();
                let new_set: std::collections::HashSet<&IndexKey> = new_keys.iter().collect();
                self.with_index_dml(storage_id, &index.name, |index_file| {
                    for key in &old_keys {
                        if !new_set
                            .get(key)
                            .is_some_and(|stored| stored.same_stored_value(key))
                        {
                            index_file.delete_deferred(txn_id, key.clone(), row_id)?;
                        }
                    }
                    for key in &new_keys {
                        if !old_set
                            .get(key)
                            .is_some_and(|stored| stored.same_stored_value(key))
                        {
                            index_file.insert_deferred(txn_id, key.clone(), row_id)?;
                        }
                    }
                    Ok(())
                })?;
                continue;
            }
            let old_key = table.index_key(index, old_values)?;
            let new_key = table.index_key(index, new_values)?;
            if old_key.same_stored_value(&new_key) {
                continue;
            }
            // Combine delete+insert into a single `IndexBatch` WAL record
            // — two ops in one batch means one log-mutex acquisition and
            // one record header instead of two. Recovery semantics are
            // identical because `redo_index_batch` already applies both
            // ops in order. This keeps single-row secondary-index updates
            // to one WAL-mutex acquisition.
            let entries = smallvec::smallvec![
                IndexBatchEntry {
                    kind: WalIndexOpKind::Delete,
                    key: old_key,
                    row_id,
                },
                IndexBatchEntry {
                    kind: WalIndexOpKind::Insert,
                    key: new_key,
                    row_id,
                },
            ];
            self.with_index_dml(storage_id, &index.name, |index_file| {
                let preencoded = index_file.preencode_mixed_index_batch_wal(txn_id, &entries)?;
                index_file.apply_preencoded_batch_deferred(txn_id, entries, preencoded)
            })?;
        }
        // Txn-buffering: bound how many heap records (and thus deferred-pinned,
        // un-evictable frames) a long-running statement parks at once. Per-row
        // boundary; a no-op when buffering is inactive or under threshold.
        self.maybe_flush_txn_wal_buffer(txn_id)?;
        Ok(())
    }

    fn update_changed_indexes_compensating(
        &self,
        storage_id: u64,
        table: &TableSchema,
        old_values: &[Value],
        new_values: &[Value],
        row_id: RowId,
    ) -> Result<(), QueryError> {
        for index in &table.indexes {
            let new_keys = table.index_keys(index, new_values)?;
            let old_keys = match table.index_keys(index, old_values) {
                Ok(keys) => keys,
                Err(_) => {
                    // The failed forward update can leave a heap value that
                    // its functional/MVI index expression cannot evaluate.
                    // Remove whatever physical entries were actually
                    // published for this row, then rebuild from the valid
                    // before-image. This is an error-only O(index) fallback.
                    let physical = self.with_index_read(storage_id, &index.name, |index_file| {
                        index_file.entries()
                    })?;
                    self.with_index_dml(storage_id, &index.name, |index_file| {
                        for (key, physical_row_id) in physical {
                            if physical_row_id == row_id {
                                index_file.delete_deferred(
                                    txn_id_for_ddl(),
                                    key,
                                    physical_row_id,
                                )?;
                            }
                        }
                        for key in &new_keys {
                            index_file.insert_deferred(txn_id_for_ddl(), key.clone(), row_id)?;
                        }
                        Ok(())
                    })?;
                    continue;
                }
            };
            let old_set: std::collections::HashSet<&IndexKey> = old_keys.iter().collect();
            let new_set: std::collections::HashSet<&IndexKey> = new_keys.iter().collect();
            self.with_index_dml(storage_id, &index.name, |index_file| {
                for key in &old_keys {
                    if !new_set
                        .get(key)
                        .is_some_and(|stored| stored.same_stored_value(key))
                    {
                        index_file.delete_deferred(txn_id_for_ddl(), key.clone(), row_id)?;
                    }
                }
                for key in &new_keys {
                    if !old_set
                        .get(key)
                        .is_some_and(|stored| stored.same_stored_value(key))
                    {
                        index_file.insert_deferred(txn_id_for_ddl(), key.clone(), row_id)?;
                    }
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    fn update_changed_index_batches_deferred(
        &self,
        txn_id: u64,
        table: &TableSchema,
        updates: &[DmlUpdate],
        changed_columns: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut by_index = table
            .indexes
            .iter()
            .filter(|index| index_may_be_affected_by_columns(table, index, changed_columns))
            .map(|index| (index.name.clone(), Vec::new()))
            .collect::<BTreeMap<_, Vec<IndexBatchEntry>>>();
        let mut repairs = Vec::new();
        for (offset, update) in updates.iter().enumerate() {
            if offset % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            for index in &table.indexes {
                if !index_may_be_affected_by_columns(table, index, changed_columns) {
                    continue;
                }
                let entries = by_index
                    .get_mut(&index.name)
                    .expect("affected index should have a batch entry");
                if index.is_fulltext() || index.has_multi_valued_parts() {
                    // Token/MVI-set diff per updated row (see the single-row path).
                    let old_keys = table.index_keys(index, &update.old_values)?;
                    let new_keys = table.index_keys(index, &update.new_values)?;
                    let old_set: std::collections::HashSet<&IndexKey> = old_keys.iter().collect();
                    let new_set: std::collections::HashSet<&IndexKey> = new_keys.iter().collect();
                    for key in &old_keys {
                        if !new_set
                            .get(key)
                            .is_some_and(|stored| stored.same_stored_value(key))
                        {
                            repairs.push((
                                IndexRepairKey {
                                    table_id: table.id,
                                    index_name: normalize_name(&index.name),
                                    key: key.clone(),
                                },
                                update.row_id,
                                false,
                            ));
                            entries.push(IndexBatchEntry {
                                kind: WalIndexOpKind::Delete,
                                key: key.clone(),
                                row_id: update.row_id,
                            });
                        }
                    }
                    for key in &new_keys {
                        if !old_set
                            .get(key)
                            .is_some_and(|stored| stored.same_stored_value(key))
                        {
                            entries.push(IndexBatchEntry {
                                kind: WalIndexOpKind::Insert,
                                key: key.clone(),
                                row_id: update.row_id,
                            });
                        }
                    }
                    continue;
                }
                let old_key = table.index_key(index, &update.old_values)?;
                let new_key = table.index_key(index, &update.new_values)?;
                if old_key.same_stored_value(&new_key) {
                    continue;
                }
                repairs.push((
                    IndexRepairKey {
                        table_id: table.id,
                        index_name: normalize_name(&index.name),
                        key: old_key.clone(),
                    },
                    update.row_id,
                    true,
                ));
                entries.push(IndexBatchEntry {
                    kind: WalIndexOpKind::Delete,
                    key: old_key,
                    row_id: update.row_id,
                });
                entries.push(IndexBatchEntry {
                    kind: WalIndexOpKind::Insert,
                    key: new_key,
                    row_id: update.row_id,
                });
            }
        }
        if !repairs.is_empty() {
            self.register_index_repair_entries(txn_id, repairs)?;
        }
        self.apply_index_batches_deferred(txn_id, table, by_index)
    }

    fn insert_row_in_table(
        &self,
        txn: TxnView,
        table: &TableSchema,
        mut values: Vec<Value>,
    ) -> Result<RowId, QueryError> {
        self.acquire_table_write_binding(txn.txn_id, table)?;
        self.assign_auto_increment_value(table, &mut values)?;
        coerce_row(table, &mut values)?;
        materialize_generated_columns(table, &mut values, None)?;
        self.check_constraints(table, txn, None, &values)?;
        let header = RowHeader::visible(txn.txn_id);
        self.preflight_insert_undo_admission(table.id, 1)?;
        let row_id = self.with_heap_mut(table.id, |heap| {
            heap.insert_mvcc(txn.txn_id, header, &values)
        })?;
        self.lock_row_nonblocking(txn.txn_id, table.id, row_id)?;
        self.push_undo_deferred(txn.txn_id, UndoKind::Insert, table.id, row_id, None, None)?;
        self.insert_indexes_deferred(txn.txn_id, table, &values, row_id)?;
        // The storage single-row API shares the same durable commit journal
        // and rollback/savepoint semantics as executor batches.
        if let Some(undo_id) = self.deferred_ann_undo_head(txn.txn_id, table)? {
            self.defer_vector_update(txn.txn_id, undo_id, table, row_id, table.id, None, &values)?;
        }
        Ok(row_id)
    }

    fn assign_auto_increment_value(
        &self,
        table: &TableSchema,
        values: &mut [Value],
    ) -> Result<(), QueryError> {
        let Some(idx) = table.auto_increment_column_index() else {
            return Ok(());
        };
        preflight_not_null_row_before_auto_increment(table, values)?;
        let initial_max = self.auto_increment_initial_max(table)?;
        if !should_assign_auto_increment(&values[idx]) {
            if let Some(explicit) = explicit_auto_increment_value_with_unsigned(
                values[idx].clone(),
                &table.columns[idx],
            )? {
                let state = self.auto_increment_state(table.id, initial_max)?;
                let previous = state.max_value.fetch_max(explicit, Ordering::AcqRel);
                if explicit > previous {
                    let version = state.version.load(Ordering::Acquire);
                    self.persist_auto_increment_state(&state, version, explicit)?;
                }
            }
            return Ok(());
        }
        let state = self.auto_increment_state(table.id, initial_max)?;
        let (step, offset) = crate::engine::expr::session_auto_increment();
        let value = loop {
            let current = state.max_value.load(Ordering::Acquire);
            let next = next_auto_increment_after(current, step, offset)?;
            if state
                .max_value
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break next;
            }
        };
        *values.get_mut(idx).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                "column count does not match value count",
            )
        })? = generated_auto_increment_value(value, &table.columns[idx])?;
        let version = state.version.load(Ordering::Acquire);
        self.persist_auto_increment_state(&state, version, value)?;
        Ok(())
    }

    fn assign_auto_increment_values(
        &self,
        txn_id: u64,
        connection_id: u32,
        table: &TableSchema,
        rows: &mut [Vec<Value>],
    ) -> Result<u64, QueryError> {
        let Some(idx) = table.auto_increment_column_index() else {
            return Ok(0);
        };
        for row in rows.iter() {
            preflight_not_null_row_before_auto_increment(table, row)?;
        }
        if rows.is_empty() {
            return Ok(0);
        }
        let column = &table.columns[idx];

        // Per-row scan decides whether the generated-only atomic range path
        // is viable. Explicit values require ordered mixed-batch semantics.
        let mut auto_required: usize = 0;
        let mut has_explicit = false;
        for values in rows.iter() {
            let current = values.get(idx).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                    "column count does not match value count",
                )
            })?;
            if should_assign_auto_increment(current) {
                auto_required += 1;
            } else if explicit_auto_increment_value_with_unsigned(current.clone(), column)?
                .is_some()
            {
                has_explicit = true;
                // Don't break — we still count subsequent
                // auto_required rows so the legacy path has the
                // right max_seen behaviour.
            }
        }
        if auto_required == 0 && !has_explicit {
            return Ok(0);
        }
        let use_atomic_batch = !has_explicit && txn_id != 0 && auto_required > 0;
        if use_atomic_batch {
            return self.assign_auto_increment_values_via_atomic_range(
                txn_id,
                connection_id,
                table,
                rows,
                idx,
                auto_required,
            );
        }
        self.assign_auto_increment_values_legacy(table, rows, idx)
    }

    /// Assign AUTO_INCREMENT values for a LOAD DATA batch that is being
    /// prepared for an unreachable shadow heap.  The normal heap-insert API
    /// performs this step internally; shadow LOAD separates identity
    /// assignment from physical append so worker threads can encode explicit
    /// identities without funnelling the whole batch through the coordinator.
    pub(crate) fn assign_load_auto_increment_values(
        &self,
        txn: TxnView,
        table: &TableSchema,
        rows: &mut [Vec<Value>],
    ) -> Result<u64, QueryError> {
        self.assign_auto_increment_values(txn.txn_id, txn.connection_id, table, rows)
    }

    /// Advance the live table's AUTO_INCREMENT high-water mark after workers
    /// have validated and encoded an explicit-only shadow LOAD batch.
    /// Persisting through the same state/WAL path as ordinary INSERT preserves
    /// crash recovery and the existing non-transactional sequence-gap
    /// semantics if the surrounding statement later rolls back.
    pub(crate) fn advance_load_auto_increment_explicit_max(
        &self,
        table: &TableSchema,
        explicit_max: u64,
    ) -> Result<(), QueryError> {
        if explicit_max == 0 || table.auto_increment_column_index().is_none() {
            return Ok(());
        }
        let initial_max = self.auto_increment_initial_max(table)?;
        let state = self.auto_increment_state(table.id, initial_max)?;
        let previous = state.max_value.fetch_max(explicit_max, Ordering::AcqRel);
        if explicit_max > previous {
            let version = state.version.load(Ordering::Acquire);
            self.persist_auto_increment_state(&state, version, explicit_max)?;
        }
        Ok(())
    }

    /// Legacy serial path: take the global Mutex, walk every row
    /// allocating sequential values for the NULL / 0 / DEFAULT
    /// slots, then persist the new max if it actually advanced.
    /// Used when an explicit user value is present in the batch
    /// (the global max needs to be brought up to that value
    /// synchronously) or when no transaction context exists.
    fn assign_auto_increment_values_legacy(
        &self,
        table: &TableSchema,
        rows: &mut [Vec<Value>],
        idx: usize,
    ) -> Result<u64, QueryError> {
        let initial_max = self.auto_increment_initial_max(table)?;
        let state = self.auto_increment_state(table.id, initial_max)?;
        let column = &table.columns[idx];
        let (step, offset) = crate::engine::expr::session_auto_increment();
        let (first_insert_id, max_seen) = loop {
            let base = state.max_value.load(Ordering::Acquire);
            let mut first_insert_id = 0;
            let mut max_seen = base;
            let mut generated = Vec::with_capacity(rows.len());
            for values in rows.iter_mut() {
                let current = values.get(idx).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                        "column count does not match value count",
                    )
                })?;
                if should_assign_auto_increment(current) {
                    let next = next_auto_increment_after(max_seen, step, offset)?;
                    generated.push(next);
                    if first_insert_id == 0 {
                        first_insert_id = next;
                    }
                    max_seen = next;
                } else if let Some(explicit) =
                    explicit_auto_increment_value_with_unsigned(current.clone(), column)?
                {
                    max_seen = max_seen.max(explicit);
                }
            }
            if state
                .max_value
                .compare_exchange_weak(base, max_seen, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let mut assigned = generated.into_iter();
            for values in rows.iter_mut() {
                if should_assign_auto_increment(&values[idx]) {
                    values[idx] = generated_auto_increment_value(
                        assigned.next().expect("generated AUTO_INCREMENT value"),
                        column,
                    )?;
                }
            }
            break (first_insert_id, max_seen);
        };
        if max_seen > initial_max {
            let version = state.version.load(Ordering::Acquire);
            self.persist_auto_increment_state(&state, version, max_seen)?;
        }
        Ok(first_insert_id)
    }

    /// Fast path: reserve the generated-only batch with one per-table atomic
    /// CAS, then publish one compact redo record before materialising values.
    ///
    /// Column dispatch is hoisted out of the loop: the per-row body is a
    /// bounds check plus one `Value` constructor.
    fn assign_auto_increment_values_via_atomic_range(
        &self,
        txn_id: u64,
        connection_id: u32,
        table: &TableSchema,
        rows: &mut [Vec<Value>],
        idx: usize,
        auto_required: usize,
    ) -> Result<u64, QueryError> {
        let column = &table.columns[idx];
        let unsigned = column.unsigned;
        // Pre-compute the column's representable max so the per-row
        // path collapses to one `u64` compare instead of going through
        // `coerce_value_with_unsigned`'s big enum match.
        let max_allowed = auto_increment_type_max(&column.data_type, unsigned)?;
        let _ = (txn_id, connection_id);
        let initial_max = self.auto_increment_initial_max(table)?;
        let state = self.auto_increment_state(table.id, initial_max)?;
        let count = auto_required as u64;
        let (step, offset) = crate::engine::expr::session_auto_increment();
        let span = (count - 1).checked_mul(step).ok_or_else(|| {
            error(
                crate::error::catalog::ER_AUTOINC_READ_FAILED,
                "auto-increment range is out of range",
            )
        })?;
        let (start_value, new_max) = loop {
            let current = state.max_value.load(Ordering::Acquire);
            let start = next_auto_increment_after(current, step, offset)?;
            let new_max = start.checked_add(span).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_AUTOINC_READ_FAILED,
                    "auto-increment value is out of range",
                )
            })?;
            if state
                .max_value
                .compare_exchange_weak(current, new_max, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break (start, new_max);
            }
        };
        let version = state.version.load(Ordering::Acquire);
        self.persist_auto_increment_state(&state, version, new_max)?;
        let mut next = start_value;
        let mut remaining = count;
        let mut first_insert_id = 0;
        for values in rows.iter_mut() {
            let current = values.get(idx).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                    "column count does not match value count",
                )
            })?;
            if should_assign_auto_increment(current) {
                if next > max_allowed {
                    return Err(error(
                        crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                        "auto-increment value out of range for column",
                    ));
                }
                // B9 — direct Value construction, no per-row enum
                // dispatch / coerce. The atomic range guarantees
                // `next > 0`, so the i64 cast is always safe for any
                // signed integer width up to BigInt.
                values[idx] = if unsigned {
                    Value::UInt(next)
                } else {
                    Value::Int(next as i64)
                };
                if first_insert_id == 0 {
                    first_insert_id = next;
                }
                remaining -= 1;
                if remaining != 0 {
                    // The reserved range was checked before the CAS. Do not
                    // advance past its last element (which may be u64::MAX).
                    next += step;
                }
            }
            // The generated-only fast path guarantees there
            // are no explicit values in this batch, so the else arm
            // from the legacy path is unreachable here.
        }
        Ok(first_insert_id)
    }

    fn cache_auto_increment_from_values(
        &self,
        table: &TableSchema,
        values: &[Value],
    ) -> Result<(), QueryError> {
        let Some(idx) = table.auto_increment_column_index() else {
            return Ok(());
        };
        let Some(explicit) =
            explicit_auto_increment_value_with_unsigned(values[idx].clone(), &table.columns[idx])?
        else {
            return Ok(());
        };
        let initial_max = self.auto_increment_initial_max(table)?;
        let state = self.auto_increment_state(table.id, initial_max)?;
        let previous = state.max_value.fetch_max(explicit, Ordering::AcqRel);
        if explicit > previous {
            let version = state.version.load(Ordering::Acquire);
            self.persist_auto_increment_state(&state, version, explicit)?;
        }
        Ok(())
    }

    fn cache_auto_increment_from_updated_values(
        &self,
        table: &TableSchema,
        values: &[Value],
        changed_columns: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        let Some(auto_increment_idx) = table.auto_increment_column_index() else {
            return Ok(());
        };
        if changed_columns.is_some_and(|columns| !columns.contains(&auto_increment_idx)) {
            return Ok(());
        }
        self.cache_auto_increment_from_values(table, values)
    }

    /// B7 — Batched version of `cache_auto_increment_from_updated_values`
    /// for multi-row UPDATE: walk the batch once finding the max
    /// explicit AI value across all `new_values`, then update the
    /// in-memory counter + persist compact redo at most once
    /// instead of per row. Skips redo when the batch's max is already
    /// covered by `cached_max`.
    fn cache_auto_increment_from_updates_batch(
        &self,
        table: &TableSchema,
        updates: &[DmlUpdate],
        changed_columns: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        let Some(auto_increment_idx) = table.auto_increment_column_index() else {
            return Ok(());
        };
        if changed_columns.is_some_and(|columns| !columns.contains(&auto_increment_idx)) {
            return Ok(());
        }
        if updates.is_empty() {
            return Ok(());
        }
        let column = &table.columns[auto_increment_idx];
        let mut batch_max = 0_u64;
        for update in updates {
            let value = update.new_values.get(auto_increment_idx).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                    "column count does not match value count",
                )
            })?;
            if let Some(explicit) =
                explicit_auto_increment_value_with_unsigned(value.clone(), column)?
            {
                if explicit > batch_max {
                    batch_max = explicit;
                }
            }
        }
        if batch_max == 0 {
            return Ok(());
        }
        let initial_max = self.auto_increment_initial_max(table)?;
        if batch_max <= initial_max {
            // Cached max already covers every explicit value in this
            // batch — no counter bump and no redo.
            return Ok(());
        }
        let state = self.auto_increment_state(table.id, initial_max)?;
        let previous = state.max_value.fetch_max(batch_max, Ordering::AcqRel);
        if batch_max > previous {
            let version = state.version.load(Ordering::Acquire);
            self.persist_auto_increment_state(&state, version, batch_max)?;
        }
        Ok(())
    }

    fn auto_increment_initial_max(&self, table: &TableSchema) -> Result<u64, QueryError> {
        if let Some(max_value) = self.auto_increment.cached_max(table.id) {
            return Ok(max_value);
        }
        // Merge concurrent cold opens before doing catalog/index I/O. Never
        // publish a provisional counter: another writer could allocate an ID
        // below the existing maximum while the first initializer is scanning.
        let initialization = self.auto_increment.initialization(table.id)?;
        let _initializing = initialization.lock();
        if let Some(max_value) = self.auto_increment.cached_max(table.id) {
            return Ok(max_value);
        }
        let disk_max = self.read_auto_increment_max(table.id)?.unwrap_or(0);
        let max_value = self.current_auto_increment_max(table)?.max(disk_max);
        let state = self.auto_increment_state(table.id, max_value)?;
        if max_value != disk_max {
            let version = state.version.load(Ordering::Acquire);
            self.persist_auto_increment_state(&state, version, max_value)?;
        }
        Ok(state.max_value.load(Ordering::Acquire))
    }

    fn current_auto_increment_max(&self, table: &TableSchema) -> Result<u64, QueryError> {
        let Some(idx) = table.auto_increment_column_index() else {
            return Ok(0);
        };
        // Invisible public indexes remain authoritative for the counter;
        // optimizer visibility must not force an internal full-table scan.
        if let Some(index) = table.iter_public_indexes().find(|index| {
            index.index_type.eq_ignore_ascii_case("BTREE")
                && index
                    .columns
                    .first()
                    .is_some_and(|name| name.eq_ignore_ascii_case(&table.columns[idx].name))
                && matches!(index.key_part(0), Some(IndexKeyPart::Column(_)))
        }) {
            if let Some(value) = self.auto_increment_max_from_index(table, index)? {
                return Ok(value);
            }
        }
        let mut max_value = 0_u64;
        // P0-1: read at the published commit frontier.
        let txn = TxnView::read_only(self.visible_scn(), IsolationLevel::ReadCommitted);
        // Only the counter column is needed. Materializing complete rows here
        // made cold initialization of wide tables consume gigabytes per writer.
        self.scan_rows_columns_streaming(&table.qualified_name(), txn, &[idx], |values| {
            if let Some(value) =
                explicit_auto_increment_value_with_unsigned(values[0].clone(), &table.columns[idx])?
            {
                max_value = max_value.max(value);
            }
            Ok(())
        })?;
        Ok(max_value)
    }

    fn auto_increment_max_from_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Result<Option<u64>, QueryError> {
        let mut max_value = 0;
        // Partitioned tables have indexes only on physical leaves, including
        // subpartitions. Opening table.id creates an empty parent index and
        // incorrectly sends every cold writer through the heap fallback.
        for storage_id in table.storage_ids() {
            let value = self.with_index_mut(storage_id, &index.name, |index_file| {
                let Some((key, _)) = index_file.last_entry()? else {
                    return Ok(Some(0));
                };
                let Some(part) = key.0.first() else {
                    return Ok(None);
                };
                auto_increment_value_from_key_part(part)
            })?;
            let Some(value) = value else {
                return Ok(None);
            };
            max_value = max_value.max(value);
        }
        Ok(Some(max_value))
    }

    fn lock_unique_keys_nonblocking<'a>(
        &self,
        txn_id: u64,
        locks: Vec<(u64, &'a str, IndexKey)>,
    ) -> Result<(), QueryError> {
        // Non-key updates and tables without unique indexes have nothing to
        // reserve. Avoid serializing an empty loop with all savepoint readers.
        if locks.is_empty() {
            return Ok(());
        }
        self.lock_manager()
            .lock_unique_keys(txn_id, locks, &self.lock_owner_status())
    }

    fn lock_unique_keys_for_values(
        &self,
        txn_id: u64,
        table: &TableSchema,
        values: &[Value],
    ) -> Result<(), QueryError> {
        if self.insert_locks_covered_by_table(txn_id, table.id) {
            return Ok(());
        }
        let mut locks = Vec::new();
        for index in table
            .indexes
            .iter()
            .filter(|index| index.enforces_uniqueness())
        {
            let key = table.index_key(index, values)?;
            if key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary {
                continue;
            }
            locks.push((table.id, index.name.as_str(), key));
        }
        self.lock_unique_keys_nonblocking(txn_id, locks)?;
        Ok(())
    }

    fn lock_unique_keys_for_rows(
        &self,
        txn_id: u64,
        table: &TableSchema,
        rows: &[Vec<Value>],
    ) -> Result<(), QueryError> {
        if self.insert_locks_covered_by_table(txn_id, table.id) {
            return Ok(());
        }
        let unique_count = table
            .indexes
            .iter()
            .filter(|index| index.enforces_uniqueness())
            .count();
        if unique_count == 1 && rows.len() > 1 {
            let index = table
                .indexes
                .iter()
                .find(|index| index.enforces_uniqueness())
                .unwrap();
            if !index.has_expression_parts()
                && index.columns.len() == 1
                && index.included_columns.is_empty()
            {
                let column = table.column_index(&index.columns[0])?;
                let number = |row: &[Value]| match row.get(column) {
                    Some(Value::Int(value)) => Some((false, i128::from(*value))),
                    Some(Value::UInt(value)) => Some((true, i128::from(*value))),
                    _ => None,
                };
                if let (Some((unsigned, first)), Some((next_unsigned, second))) =
                    (number(&rows[0]), number(&rows[1]))
                {
                    let step = second - first;
                    if unsigned == next_unsigned
                        && (step == 1 || step == -1)
                        && rows.iter().enumerate().all(|(n, row)| {
                            number(row) == Some((unsigned, first + step * n as i128))
                        })
                    {
                        let key = table.index_key(index, &rows[0])?;
                        let canonical = match key.0.as_slice() {
                            [KeyPart::Int(value)] => !unsigned && i128::from(*value) == first,
                            [KeyPart::UInt(value)] => unsigned && i128::from(*value) == first,
                            _ => false,
                        };
                        let last = first + step * (rows.len() - 1) as i128;
                        if canonical
                            && self.lock_manager().try_lock_integer_range(
                                txn_id,
                                table.id,
                                &index.name,
                                unsigned,
                                first.min(last),
                                first.max(last),
                            )?
                        {
                            return Ok(());
                        }
                    }
                }
            }
        }
        let mut locks = Vec::with_capacity(rows.len().saturating_mul(unique_count));
        for values in rows {
            for index in table
                .indexes
                .iter()
                .filter(|index| index.enforces_uniqueness())
            {
                let key = table.index_key(index, values)?;
                if key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary {
                    continue;
                }
                locks.push((table.id, index.name.as_str(), key));
            }
        }
        self.lock_unique_keys_nonblocking(txn_id, locks)?;
        Ok(())
    }

    fn lock_changed_unique_keys_for_update(
        &self,
        txn_id: u64,
        table: &TableSchema,
        old_values: &[Value],
        new_values: &[Value],
        changed_columns: Option<&[usize]>,
        affected_index_positions: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        let mut locks = Vec::new();
        for (index_pos, index) in table.indexes.iter().enumerate() {
            if !index.enforces_uniqueness()
                || !affected_index_positions.map_or_else(
                    || index_may_be_affected_by_columns(table, index, changed_columns),
                    |positions| positions.binary_search(&index_pos).is_ok(),
                )
            {
                continue;
            }
            let old_key = table.index_key(index, old_values)?;
            let new_key = table.index_key(index, new_values)?;
            if old_key == new_key {
                continue;
            }
            if !old_key.0.iter().any(|part| matches!(part, KeyPart::Null)) || index.primary {
                locks.push((table.id, index.name.as_str(), old_key));
            }
            if new_key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary {
                continue;
            }
            locks.push((table.id, index.name.as_str(), new_key));
        }
        self.lock_unique_keys_nonblocking(txn_id, locks)?;
        Ok(())
    }

    fn lock_update_rows(
        &self,
        txn_id: u64,
        table: &TableSchema,
        updates: &[DmlUpdate],
        changed_columns: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        let mut row_ids = Vec::with_capacity(updates.len());
        let mut unique_locks = Vec::new();
        for (offset, update) in updates.iter().enumerate() {
            if offset % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            row_ids.push(update.row_id);
            self.collect_changed_unique_key_locks_for_update(
                table,
                &update.old_values,
                &update.new_values,
                changed_columns,
                &mut unique_locks,
            )?;
        }
        // Non-blocking: the caller already captured `update.old_values` from
        // an MVCC read; if the lock conflicts, we must propagate
        // LOCK_WAIT_PENDING up so the executor reruns the statement and
        // re-reads at the latest committed version. Blocking here would
        // produce lost updates against `d_ytd += amount`-style writes.
        self.lock_rows_nonblocking(txn_id, table.id, row_ids)?;
        self.lock_unique_keys_nonblocking(txn_id, unique_locks)?;
        Ok(())
    }

    fn lock_delete_rows(
        &self,
        txn_id: u64,
        table: &TableSchema,
        deletes: &[DmlDelete],
    ) -> Result<(), QueryError> {
        let mut row_ids = Vec::with_capacity(deletes.len());
        let mut unique_locks = Vec::new();
        for (offset, delete) in deletes.iter().enumerate() {
            if offset % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            row_ids.push(delete.row_id);
            self.collect_unique_key_locks_for_values(table, &delete.values, &mut unique_locks)?;
        }
        // Non-blocking: deletes captured `delete.values` from an MVCC read;
        // see `lock_update_rows` for the reasoning.
        self.lock_rows_nonblocking(txn_id, table.id, row_ids)?;
        self.lock_unique_keys_nonblocking(txn_id, unique_locks)?;
        Ok(())
    }

    fn collect_unique_key_locks_for_values<'a>(
        &self,
        table: &'a TableSchema,
        values: &[Value],
        locks: &mut Vec<(u64, &'a str, IndexKey)>,
    ) -> Result<(), QueryError> {
        for index in table
            .indexes
            .iter()
            .filter(|index| index.enforces_uniqueness())
        {
            let key = table.index_key(index, values)?;
            if key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary {
                continue;
            }
            locks.push((table.id, index.name.as_str(), key));
        }
        Ok(())
    }

    fn collect_changed_unique_key_locks_for_update<'a>(
        &self,
        table: &'a TableSchema,
        old_values: &[Value],
        new_values: &[Value],
        changed_columns: Option<&[usize]>,
        locks: &mut Vec<(u64, &'a str, IndexKey)>,
    ) -> Result<(), QueryError> {
        for index in table
            .indexes
            .iter()
            .filter(|index| index.enforces_uniqueness())
        {
            if !index_may_be_affected_by_columns(table, index, changed_columns) {
                continue;
            }
            let old_key = table.index_key(index, old_values)?;
            let new_key = table.index_key(index, new_values)?;
            if old_key == new_key {
                continue;
            }
            if !old_key.0.iter().any(|part| matches!(part, KeyPart::Null)) || index.primary {
                locks.push((table.id, index.name.as_str(), old_key));
            }
            if new_key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary {
                continue;
            }
            locks.push((table.id, index.name.as_str(), new_key));
        }
        Ok(())
    }

    /// Drops the index entry from the per-partition index file at
    /// `storage_id`. Repair-key registration stays bound to the
    /// logical `table.id`.
    pub(crate) fn delete_indexes_deferred_at(
        &self,
        txn_id: u64,
        storage_id: u64,
        table: &TableSchema,
        values: &[Value],
        row_id: RowId,
    ) -> Result<(), QueryError> {
        let mut repairs = Vec::new();
        let mut deletes = Vec::new();
        for index in &table.indexes {
            for key in table.index_keys(index, values)? {
                repairs.push((
                    IndexRepairKey {
                        table_id: table.id,
                        index_name: normalize_name(&index.name),
                        key: key.clone(),
                    },
                    !index.is_fulltext() && !index.has_multi_valued_parts(),
                ));
                deletes.push((index, key));
            }
        }
        if !repairs.is_empty() {
            self.register_index_repair_entries(
                txn_id,
                repairs
                    .into_iter()
                    .map(|(key, single_key)| (key, row_id, single_key)),
            )?;
        }
        // Publish old-key ownership before removing physical entries, as the
        // batched DELETE and key-changing UPDATE paths already do. Otherwise
        // a concurrent snapshot can probe the gap before its repair exists.
        for (index, key) in deletes {
            self.with_index_dml(storage_id, &index.name, |index_file| {
                index_file.delete_deferred(txn_id, key, row_id)
            })?;
        }
        Ok(())
    }

    fn delete_index_batches_deferred(
        &self,
        txn_id: u64,
        table: &TableSchema,
        deletes: &[DmlDelete],
    ) -> Result<(), QueryError> {
        if deletes.is_empty() {
            return Ok(());
        }
        let mut by_index = table
            .indexes
            .iter()
            .map(|index| (index.name.clone(), Vec::new()))
            .collect::<BTreeMap<_, Vec<IndexBatchEntry>>>();
        let mut repairs = Vec::new();
        for (offset, delete) in deletes.iter().enumerate() {
            if offset % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            for index in &table.indexes {
                let batch = by_index
                    .get_mut(&index.name)
                    .expect("index should have a batch entry");
                for key in table.index_keys(index, &delete.values)? {
                    batch.push(IndexBatchEntry {
                        kind: WalIndexOpKind::Delete,
                        key: key.clone(),
                        row_id: delete.row_id,
                    });
                    repairs.push((
                        IndexRepairKey {
                            table_id: table.id,
                            index_name: normalize_name(&index.name),
                            key,
                        },
                        delete.row_id,
                        !index.is_fulltext() && !index.has_multi_valued_parts(),
                    ));
                }
            }
        }
        if !repairs.is_empty() {
            self.register_index_repair_entries(txn_id, repairs)?;
        }
        self.apply_index_batches_deferred(txn_id, table, by_index)
    }

    fn apply_index_batches_deferred(
        &self,
        txn_id: u64,
        table: &TableSchema,
        batches: BTreeMap<String, Vec<IndexBatchEntry>>,
    ) -> Result<(), QueryError> {
        self.apply_index_batches_deferred_at(txn_id, table.id, table, batches)
    }

    fn apply_index_batches_deferred_at(
        &self,
        txn_id: u64,
        storage_id: u64,
        table: &TableSchema,
        mut batches: BTreeMap<String, Vec<IndexBatchEntry>>,
    ) -> Result<(), QueryError> {
        for index in &table.indexes {
            let entries = batches.remove(&index.name).unwrap_or_default();
            if entries.is_empty() {
                continue;
            }
            self.with_index_dml(storage_id, &index.name, |index_file| {
                index_file.apply_batch_deferred(txn_id, entries)
            })?;
        }
        Ok(())
    }

    pub(super) fn rebuild_indexes(&self, table: &TableSchema) -> Result<(), QueryError> {
        let names = table
            .indexes
            .iter()
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        self.rebuild_indexes_offline_bulk_batch_with_options(
            table,
            &names,
            IndexBuildOptions::default(),
        )
    }
    fn rebuild_index(&self, table: &TableSchema, index: &IndexSchema) -> Result<(), QueryError> {
        self.rebuild_index_with_options(table, index, IndexBuildOptions::default())
    }

    fn rebuild_index_with_options(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        options: IndexBuildOptions,
    ) -> Result<(), QueryError> {
        // Online ADD INDEX previously used the generic external sorter without
        // a governor owner. On wide/composite keys its source Vec, parallel
        // merge Vec and allocator-retained pages could therefore coexist with
        // a full buffer pool and let the kernel OOM the server. Acquire one
        // statement-scoped StorageScratch reservation and keep it alive until
        // the sorted entries have been consumed by the index writer.
        let minimum = 16 * 1024 * 1024;
        let requested_scratch =
            crate::engine::executor::mem_budget::query_mem_limit_bytes().max(minimum);
        let (_buffer_lease, _scratch_charge, scratch_bytes) =
            reserve_offline_index_build_scratch(self, requested_scratch, minimum)?;
        let entries = self.collect_sorted_index_entries_with_degree(
            table,
            index,
            options.effective_degree,
            scratch_bytes,
        )?;

        // Phase 3: chunked incremental backfill. Each chunk takes the
        // index-file write lock once; between chunks DML can take the
        // lock and insert its own entries. `insert_batch_deferred`
        // optimises the empty-tree case via bulk_load_sorted, so the
        // first chunk is fast even when the table is large.
        const BACKFILL_CHUNK: usize = 4096;
        match entries {
            SortedIndexEntries::Memory(entries) => {
                for (chunk_no, chunk) in entries.chunks(BACKFILL_CHUNK).enumerate() {
                    let owned: Vec<(IndexKey, RowId)> =
                        chunk.iter().map(|e| (e.key.clone(), e.row_id)).collect();
                    self.with_index_mut(table.id, &index.name, |index_file| {
                        // Exclusive (write) handle lock per chunk excludes
                        // every concurrent navigator, so the empty-tree
                        // bulk-load reset on the first chunk is safe.
                        index_file.insert_batch_deferred(txn_id_for_ddl(), owned, true)
                    })?;
                    if chunk_no % 64 == 63 {
                        crate::engine::memory_reclaimer::reclaim_bulk_operation_pressure();
                    }
                }
            }
            SortedIndexEntries::Spilled(runs) => {
                let mut merger = runs.into_merger()?;
                let mut chunk_no = 0usize;
                loop {
                    let mut owned: Vec<(IndexKey, RowId)> = Vec::with_capacity(BACKFILL_CHUNK);
                    while owned.len() < BACKFILL_CHUNK {
                        let Some(entry) = merger.next_entry()? else {
                            break;
                        };
                        owned.push((entry.key, entry.row_id));
                    }
                    if owned.is_empty() {
                        break;
                    }
                    self.with_index_mut(table.id, &index.name, |index_file| {
                        // Exclusive (write) handle lock per chunk: safe to
                        // take the empty-tree bulk-load reset fast path.
                        index_file.insert_batch_deferred(txn_id_for_ddl(), owned, true)
                    })?;
                    chunk_no = chunk_no.saturating_add(1);
                    if chunk_no % 64 == 0 {
                        crate::engine::memory_reclaimer::reclaim_bulk_operation_pressure();
                    }
                }
            }
        }
        // Flush dirty index pages so the post-DDL on-disk file
        // observes the backfilled tree without waiting for the next
        // checkpoint. The legacy bulk-load path wrote pages directly;
        // the incremental path goes through the buffer pool, so an
        // explicit flush is required to match the durability that
        // ADD INDEX advertised before this refactor.
        self.with_index_exclusive(table.id, &index.name, |index_file| index_file.flush())?;
        // For UNIQUE indexes, run a finalize-time scan of the
        // backfilled tree to catch a corner case the pre-Phase-1
        // `validate_existing_unique_index_values` pre-check cannot
        // see: a concurrent DML INSERT that landed during Phase 2/3
        // with a key that conflicts with a heap row backfill was
        // about to insert. DML's own UNIQUE check looks up the
        // (still-being-built) index; if backfill hadn't yet
        // populated the conflicting heap row's entry, the lookup
        // misses and the INSERT slips through. Walking the finished
        // tree once at the end closes the window. NULL keys repeat
        // freely in non-PRIMARY UNIQUE indexes per the same rule the
        // DML path applies.
        if index.unique {
            let entries = self.with_index_read(table.id, &index.name, |idx| idx.entries())?;
            let mut prev_key: Option<IndexKey> = None;
            for (key, _row_id) in entries {
                let is_null = key.0.iter().any(|p| matches!(p, KeyPart::Null));
                if is_null && index.primary {
                    return Err(error(
                        crate::error::catalog::ER_INVALID_USE_OF_NULL,
                        "invalid use of NULL value for primary key",
                    ));
                }
                if is_null && !index.primary {
                    prev_key = None;
                    continue;
                }
                if let Some(prev) = &prev_key {
                    if prev == &key {
                        return Err(error(
                            crate::error::catalog::ER_DUP_ENTRY,
                            format!("duplicate entry for key '{}'", index.name),
                        ));
                    }
                }
                prev_key = Some(key);
            }
        }
        Ok(())
    }

    pub(super) fn rebuild_index_offline_bulk(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Result<(), QueryError> {
        self.rebuild_index_offline_bulk_with_options(table, index, IndexBuildOptions::default())
    }

    pub(super) fn rebuild_index_offline_bulk_with_options(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        options: IndexBuildOptions,
    ) -> Result<(), QueryError> {
        if index.index_type.eq_ignore_ascii_case("BTREE") {
            INDEX_BUILD_REQUESTED_WORKERS.fetch_add(
                options.requested_degree.min(u64::MAX as usize) as u64,
                Ordering::Relaxed,
            );
            INDEX_BUILD_EFFECTIVE_WORKERS.fetch_add(
                options.effective_degree.min(u64::MAX as usize) as u64,
                Ordering::Relaxed,
            );
            if options.effective_degree > 1 {
                INDEX_BUILD_PARALLEL_BUILDS.fetch_add(1, Ordering::Relaxed);
            } else {
                INDEX_BUILD_SERIAL_BUILDS.fetch_add(1, Ordering::Relaxed);
            }
            if options.effective_degree < options.requested_degree {
                INDEX_BUILD_DEGREE_DOWNGRADES.fetch_add(1, Ordering::Relaxed);
            }
        }
        // Keep Memory durability on its existing buffer-pool scan path. The
        // strict/relaxed offline scanner below is now hybrid: resident pages
        // come from their coherent buffer image and cold pages are read
        // directly without cache admission.
        if self.config.durability == crate::storage::DurabilityMode::Memory {
            return self.rebuild_index_with_options(table, index, options);
        }
        if encoded_btree_build_eligible(index) {
            return self.rebuild_indexes_offline_encoded_batch(
                table,
                std::slice::from_ref(&index.name),
                options,
            );
        }
        self.remove_index_cache_entry(table.id, &index.name)?;
        let path = self.index_path_for_table_schema(table, table.id, &index.name);
        self.buffer_pool
            .discard_file(&Arc::new(BufferPool::index_file_key(path.clone())))?;
        let minimum = 16 * 1024 * 1024;
        let requested_scratch =
            crate::engine::executor::mem_budget::query_mem_limit_bytes().max(minimum);
        let (_buffer_lease, _scratch_charge, scratch_bytes) =
            reserve_offline_index_build_scratch(self, requested_scratch, minimum)?;
        let entries = self.collect_sorted_index_entries_direct_with_degree(
            table,
            index,
            options.effective_degree,
            scratch_bytes,
        )?;
        let index_file = match entries {
            SortedIndexEntries::Memory(entries) => {
                validate_unique_memory_entries(&entries, index)?;
                IndexFile::create_rebuilt_from_sorted_with_durability(
                    path,
                    entries,
                    self.log.clone(),
                    self.config.durability,
                    self.buffer_pool.clone(),
                )?
            }
            SortedIndexEntries::Spilled(runs) => {
                let merger = runs.into_merger()?;
                let stream: Box<dyn Iterator<Item = Result<IndexEntry, QueryError>>> =
                    if index.unique {
                        Box::new(UniqueCheckedIndexEntries::new(merger, index))
                    } else {
                        Box::new(merger)
                    };
                IndexFile::create_rebuilt_from_sorted_stream_with_durability(
                    path,
                    stream,
                    self.log.clone(),
                    self.config.durability,
                    self.buffer_pool.clone(),
                )?
            }
        };
        index_file.flush()?;
        self.remove_index_cache_entry(table.id, &index.name)?;
        Ok(())
    }

    /// Vector companion indexes contain only (NULL, RowId). Keep this header-
    /// only scan, but use the same governed external sorter and streaming writer
    /// as other keys: materializing a second million-entry Vec bypasses spill.
    fn collect_vector_registry_entries(
        &self,
        table: &TableSchema,
        index: Option<&IndexSchema>,
        degree: usize,
        scratch_bytes: usize,
        direct: bool,
    ) -> Result<SortedIndexEntries, QueryError> {
        let mut sorter = IndexEntryExternalSorter::with_exact_run_bytes(scratch_bytes);
        let key = IndexKey::from(vec![KeyPart::Null]);
        let cosine = index.is_some_and(|i| {
            i.vector_meta_for_table(table).metric_enum() == vector::Metric::Cosine
        });
        let columns = if cosine {
            vec![table.column_index(&index.unwrap().columns[0])?]
        } else {
            Vec::new()
        };
        self.with_heap_read(table.id, |heap| {
            let mut append = |row: ProjectedMvccStoredRow<'_>| {
                if !row.header.is_deleted() {
                    if cosine&&matches!(row.values.first(),Some(Value::Vector(value)) if vector::distance::typed_is_zero(value)){return Err(vector::vector_err("COSINE indexes reject existing zero vectors"));}
                    sorter.push(
                        self.data_dir(),
                        IndexEntry {
                            key: key.clone(),
                            row_id: row.row_id,
                        },
                    )?;
                }
                Ok(false)
            };
            if direct {
                heap.scan_mvcc_columns_direct_until(&columns, &mut append)
            } else {
                heap.scan_mvcc_columns_until(&columns, &mut append)
            }
        })?;
        sorter.finish_with_degree(self.data_dir(), degree)
    }

    fn collect_sorted_index_entries_with_scan<const DIRECT: bool>(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        degree: usize,
        scratch_bytes: usize,
    ) -> Result<SortedIndexEntries, QueryError> {
        if index.is_vector() {
            return self.collect_vector_registry_entries(
                table,
                Some(index),
                degree,
                scratch_bytes,
                DIRECT,
            );
        }
        if index.is_fulltext() || index.has_multi_valued_parts() {
            return self.collect_sorted_multi_key_entries(table, index, scratch_bytes);
        }
        let data_dir = self.data_dir();
        let mut sorter = IndexEntryExternalSorter::with_exact_run_bytes(scratch_bytes);
        // Functional indexes (`INDEX ((LOWER(c)))`) and SPATIAL indexes need
        // their schema-specific key builder: functional parts evaluate an
        // expression and SPATIAL parts encode four MBR coordinates. Their
        // declared column names cannot be projected into ordinary scalar key
        // parts without changing the persisted key representation, so
        // `effective_columns()` would fail the `column_index` lookup.
        // Project the full row and route through `TableSchema::index_key`
        // — the same path the live INSERT/UPDATE writers take — so the
        // backfilled keys (including evaluated expressions and prefix
        // truncation) match bit-for-bit.
        if index.has_expression_parts() || index.is_spatial() {
            self.with_heap_read(table.id, |heap| {
                let mut visit = |row: ProjectedMvccStoredRow<'_>| {
                    if !row.header.is_deleted() {
                        for key in table.index_keys(index, row.values)? {
                            sorter.push(
                                data_dir,
                                IndexEntry {
                                    key,
                                    row_id: row.row_id,
                                },
                            )?;
                        }
                    }
                    Ok(false)
                };
                if DIRECT {
                    heap.scan_mvcc_columns_direct_until(
                        &(0..table.columns.len()).collect::<Vec<_>>(),
                        &mut visit,
                    )
                } else {
                    heap.scan_mvcc_columns_until(
                        &(0..table.columns.len()).collect::<Vec<_>>(),
                        &mut visit,
                    )
                }
            })?;
            return sorter.finish_with_degree(data_dir, degree);
        }
        // Plain column index — project just the key columns and build
        // each KeyPart directly. The effective_columns() projection
        // matches `table.index_key`'s layout (declared columns followed
        // by INCLUDE columns) so the backfilled keys are byte-identical
        // to what the live insert path would produce.
        let column_indexes = index
            .effective_columns()
            .iter()
            .map(|column| table.column_index(column))
            .collect::<Result<Vec<_>, _>>()?;
        self.with_heap_read(table.id, |heap| {
            let mut visit = |row: ProjectedMvccStoredRow<'_>| {
                if !row.header.is_deleted() {
                    let key = IndexKey::from(
                        row.values
                            .iter()
                            .zip(column_indexes.iter())
                            .enumerate()
                            .map(|(pos, (value, idx))| {
                                index.apply_prefix_to_key_part(
                                    pos,
                                    KeyPart::from_column_value_with_collation(
                                        value,
                                        &table.columns[*idx].data_type,
                                        table.columns[*idx].collation.as_deref(),
                                    ),
                                )
                            })
                            .collect::<Vec<_>>(),
                    );
                    sorter.push(
                        data_dir,
                        IndexEntry {
                            key,
                            row_id: row.row_id,
                        },
                    )?;
                }
                Ok(false)
            };
            if DIRECT {
                heap.scan_mvcc_columns_direct_until(&column_indexes, &mut visit)
            } else {
                heap.scan_mvcc_columns_until(&column_indexes, &mut visit)
            }
        })?;
        sorter.finish_with_degree(data_dir, degree)
    }

    fn collect_sorted_index_entries_with_degree(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        degree: usize,
        scratch_bytes: usize,
    ) -> Result<SortedIndexEntries, QueryError> {
        self.collect_sorted_index_entries_with_scan::<false>(table, index, degree, scratch_bytes)
    }

    /// Collect one row's multi-key index entries for backfill (FULLTEXT tokens or JSON MVI elements).
    fn collect_sorted_multi_key_entries(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        scratch_bytes: usize,
    ) -> Result<SortedIndexEntries, QueryError> {
        let data_dir = self.data_dir();
        let mut sorter = IndexEntryExternalSorter::with_exact_run_bytes(scratch_bytes);
        self.with_heap_read(table.id, |heap| {
            heap.scan_mvcc_columns_direct_until(
                &(0..table.columns.len()).collect::<Vec<_>>(),
                |row| {
                    if !row.header.is_deleted() {
                        for key in table.index_keys(index, row.values)? {
                            sorter.push(
                                data_dir,
                                IndexEntry {
                                    key,
                                    row_id: row.row_id,
                                },
                            )?;
                        }
                    }
                    Ok(false)
                },
            )
        })?;
        sorter.finish(data_dir)
    }

    fn collect_sorted_index_entries_direct_with_degree(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        degree: usize,
        scratch_bytes: usize,
    ) -> Result<SortedIndexEntries, QueryError> {
        self.collect_sorted_index_entries_with_scan::<true>(table, index, degree, scratch_bytes)
    }

    pub(super) fn prepare_empty_index_file_at_path(
        &self,
        table_id: u64,
        index_name: &str,
        path: PathBuf,
    ) -> Result<(), QueryError> {
        self.remove_index_cache_entry(table_id, index_name)?;
        let _ = fs::remove_file(&path);
        let empty = IndexFile::create_rebuilt_from_sorted_with_durability(
            path,
            Vec::new(),
            self.log.clone(),
            self.config.durability,
            self.buffer_pool.clone(),
        )?;
        empty.flush()?;
        self.remove_index_cache_entry(table_id, index_name)
    }

    pub(crate) fn index_build_row_count_hint(&self, table: &TableSchema) -> usize {
        let effective = (table.stats.row_count as i128
            + self.table_live_row_delta(table.id) as i128)
            .clamp(0, usize::MAX as i128);
        if effective > 0 {
            return effective as usize;
        }
        // Live-row deltas are intentionally in-memory and may be zero after a
        // clean restart of a freshly bulk-loaded, not-yet-ANALYZEd table. A
        // zero hint made the offline sorter request only the per-query soft
        // default, spill halfway through a 60M-row index, and then expand the
        // compact dictionary run back to disk. Use the physical heap length as
        // a conservative capacity hint in that case. Forty-eight bytes is
        // below the encoded MVCC+slot footprint of ordinary multi-column rows;
        // Use this estimate only when neither persisted statistics nor live
        // deltas provide a row count. Wide rows would otherwise inflate the
        // estimate well beyond the actual number of sortable records.
        // Underestimation retains the external-spill correctness path.
        const CONSERVATIVE_PHYSICAL_ROW_BYTES: u64 = 48;
        let physical = std::fs::metadata(self.table_path(table.id))
            .map(|meta| meta.len() / CONSERVATIVE_PHYSICAL_ROW_BYTES)
            .unwrap_or(0)
            .min(usize::MAX as u64) as usize;
        physical
    }

    /// Build algorithm-specific indexes independently and route every B-tree
    /// in the statement through the same compiled encoded builder.
    fn rebuild_indexes_offline_bulk_batch_spilled(
        &self,
        table: &TableSchema,
        index_names: &[String],
        options: IndexBuildOptions,
    ) -> Result<(), QueryError> {
        let mut btree_names = Vec::with_capacity(index_names.len());
        for name in index_names {
            let index = table
                .indexes
                .iter()
                .find(|index| index.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("batched ADD INDEX missing index '{name}' in cloned schema"),
                    )
                })?;
            if encoded_btree_build_eligible(index) {
                btree_names.push(name.clone());
            } else {
                self.rebuild_index_offline_bulk_with_options(table, index, options)?;
            }
        }
        if !btree_names.is_empty() {
            self.rebuild_indexes_offline_encoded_batch(table, &btree_names, options)?;
        }
        Ok(())
    }

    /// Offline ADD INDEX entry point. Persistent modes share one encoded
    /// multi-index scan; memory durability keeps its coherent per-index scan.
    fn rebuild_indexes_offline_bulk_batch_with_options(
        &self,
        table: &TableSchema,
        index_names: &[String],
        options: IndexBuildOptions,
    ) -> Result<(), QueryError> {
        if self.config.durability != crate::storage::DurabilityMode::Memory {
            return self.rebuild_indexes_offline_bulk_batch_spilled(table, index_names, options);
        }
        for name in index_names {
            let index = table
                .indexes
                .iter()
                .find(|index| index.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("batched ADD INDEX missing index '{name}' in cloned schema"),
                    )
                })?;
            self.rebuild_index_offline_bulk_with_options(table, index, options)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod concurrent_load_publish_tests {
    use super::*;

    fn primary_int_column() -> ColumnDef {
        ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }
    }

    fn sorted_entries(values: &[i64], page_id: u32) -> LoadDataSortedEntries {
        LoadDataSortedEntries::Generic(SortedIndexEntries::Memory(
            values
                .iter()
                .enumerate()
                .map(|(slot, value)| IndexEntry {
                    key: IndexKey::from(vec![KeyPart::Int(*value)]),
                    row_id: RowId {
                        page_id,
                        slot_id: slot as u16,
                    },
                })
                .collect(),
        ))
    }

    #[test]
    fn initially_empty_load_plan_merges_if_another_loader_publishes_first() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Memory).unwrap();
        db.create_table("t", vec![primary_int_column()], vec!["id".into()], vec![])
            .unwrap();
        let table = db.table("t").unwrap();
        let index = table
            .indexes
            .iter()
            .find(|candidate| candidate.primary)
            .unwrap()
            .clone();

        // Both plans made the same initially-empty decision. The first
        // publication builds the tree; the second must merge, not attempt a
        // second destructive empty-tree build.
        LoadDataIndexBuilders::finish_empty_plan(
            &db,
            table.id,
            1,
            index.clone(),
            None,
            sorted_entries(&[1, 3], 1),
        )
        .unwrap();
        LoadDataIndexBuilders::finish_empty_plan(
            &db,
            table.id,
            2,
            index.clone(),
            None,
            sorted_entries(&[2, 4], 2),
        )
        .unwrap();

        let entries = db
            .with_index_read(table.id, &index.name, IndexFile::entries)
            .unwrap();
        let values = entries
            .into_iter()
            .map(|(key, _)| key.0[0].clone())
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![
                KeyPart::Int(1),
                KeyPart::Int(2),
                KeyPart::Int(3),
                KeyPart::Int(4)
            ]
        );

        let duplicate = LoadDataIndexBuilders::finish_empty_plan(
            &db,
            table.id,
            3,
            index,
            None,
            sorted_entries(&[2], 3),
        )
        .unwrap_err();
        assert_eq!(duplicate.code(), crate::error::catalog::ER_DUP_ENTRY.code());
    }
}
