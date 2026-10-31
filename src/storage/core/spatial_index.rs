//! Page-backed R-tree. Page batches are redo-only; row undo performs logical compensation.
use super::*;
use crate::engine::geo::Mbr;

pub(super) const MAGIC: u32 = 0x52545031;
const VERSION: u16 = crate::storage::format_versions::SPATIAL_PAGE_VERSION;
const HEADER: usize = 64;
const CAPACITY: usize = (PAGE_SIZE - HEADER) / 40;
const MIN_FILL: usize = CAPACITY * 2 / 5;
const MAX_BATCH_PAGES: usize = 128;

#[cfg(test)]
thread_local! { pub(super) static CRASH_AFTER_PAGES: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) }; }
#[cfg(test)]
thread_local! { pub(super) static CRASH_STRUCTURAL_ONLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(test)]
fn crash_point(log: &Arc<crate::wal::LogMutex>, lsn: u64) {
    CRASH_AFTER_PAGES.with(|point| {
        if let Some(remaining) = point.get() {
            if remaining == 0 {
                LogManager::sync_durable_to(log, lsn).unwrap();
                std::process::exit(86);
            }
            point.set(Some(remaining - 1));
        }
    });
}

fn corrupt(message: &str) -> QueryError {
    error(
        crate::error::catalog::ER_INDEX_CORRUPT,
        format!("corrupt spatial index: {message}"),
    )
}

#[derive(Clone, Copy, Debug)]
struct Item {
    mbr: Mbr,
    pointer: u64,
}
#[derive(Clone, Debug)]
struct Node {
    level: u16,
    items: Vec<Item>,
}
impl Node {
    fn cover(&self) -> Option<Mbr> {
        self.items.iter().map(|e| e.mbr).reduce(|a, b| a.union(&b))
    }
    fn encode(&self) -> Result<Vec<u8>, QueryError> {
        if self.items.len() > CAPACITY {
            return Err(corrupt("page overflow"));
        }
        let mut bytes = page(1);
        write_u16(&mut bytes, 26, self.level);
        write_u16(&mut bytes, 28, self.items.len() as u16);
        for (i, e) in self.items.iter().enumerate() {
            let p = HEADER + i * 40;
            for (j, v) in [e.mbr.minx, e.mbr.miny, e.mbr.maxx, e.mbr.maxy]
                .iter()
                .enumerate()
            {
                write_u64(&mut bytes, p + j * 8, v.to_bits());
            }
            write_u64(&mut bytes, p + 32, e.pointer);
        }
        refresh_page_checksum(&mut bytes);
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self, QueryError> {
        validate_page(bytes)?;
        if bytes[24] != 1 {
            return Err(corrupt("expected tree page"));
        }
        let count = read_u16(bytes, 28) as usize;
        if count > CAPACITY {
            return Err(corrupt("invalid entry count"));
        }
        let mut items = Vec::with_capacity(count);
        for i in 0..count {
            let p = HEADER + i * 40;
            let mbr = Mbr::new(
                f64::from_bits(read_u64(bytes, p)),
                f64::from_bits(read_u64(bytes, p + 8)),
                f64::from_bits(read_u64(bytes, p + 16)),
                f64::from_bits(read_u64(bytes, p + 24)),
            );
            validate_mbr(mbr)?;
            items.push(Item {
                mbr,
                pointer: read_u64(bytes, p + 32),
            });
        }
        let level = read_u16(bytes, 26);
        if level > 64 {
            return Err(corrupt("invalid tree level"));
        }
        if level == 0 && items.iter().any(|e| e.pointer >> 48 != 0) {
            return Err(corrupt("invalid row id"));
        }
        Ok(Self { level, items })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Meta {
    root: u64,
    next: u64,
    free: u64,
    index_id: u64,
    height: u16,
}
impl Meta {
    fn encode(self) -> Vec<u8> {
        let mut bytes = page(0);
        write_u64(&mut bytes, 64, self.root);
        write_u64(&mut bytes, 72, self.next);
        write_u64(&mut bytes, 80, self.free);
        write_u64(&mut bytes, 88, self.index_id);
        write_u16(&mut bytes, 96, self.height);
        refresh_page_checksum(&mut bytes);
        bytes
    }
    fn decode(bytes: &[u8]) -> Result<Self, QueryError> {
        validate_page(bytes)?;
        if bytes[24] != 0 {
            return Err(corrupt("expected meta page"));
        }
        let m = Self {
            root: read_u64(bytes, 64),
            next: read_u64(bytes, 72),
            free: read_u64(bytes, 80),
            index_id: read_u64(bytes, 88),
            height: read_u16(bytes, 96),
        };
        if m.root == 0 || m.root >= m.next || m.free >= m.next || m.height == 0 || m.height > 65 {
            return Err(corrupt("invalid root or allocator"));
        }
        Ok(m)
    }
}
// Stable index IDs survive name changes and shadow-generation relocation.
// Temporary indexes use session-owned names and carry the reserved ID zero.
fn persistent_index_id(path: &Path) -> u64 {
    crate::storage::naming::parse_relative_object_path(&IndexFile::compute_relative_path(path))
        .and_then(|(_, info)| info.part.parse().ok())
        .unwrap_or(0)
}

fn page(kind: u8) -> Vec<u8> {
    let mut p = vec![0; PAGE_SIZE];
    write_u32(&mut p, 0, MAGIC);
    write_u16(&mut p, 4, VERSION);
    write_u32(&mut p, 20, (PAGE_SIZE - 24) as u32);
    p[24] = kind;
    p
}
pub(super) fn validate_page(bytes: &[u8]) -> Result<(), QueryError> {
    if bytes.len() != PAGE_SIZE
        || read_u32(bytes, 0) != MAGIC
        || read_u16(bytes, 4) != VERSION
        || read_u32(bytes, 16) != page_checksum(bytes, 16)
    {
        return Err(corrupt("invalid page format or checksum"));
    }
    Ok(())
}
fn validate_mbr(m: Mbr) -> Result<(), QueryError> {
    if [m.minx, m.miny, m.maxx, m.maxy]
        .iter()
        .any(|x| !x.is_finite())
        || m.minx > m.maxx
        || m.miny > m.maxy
    {
        return Err(corrupt("invalid MBR"));
    }
    Ok(())
}
fn decode_key(key: &IndexKey) -> Result<Mbr, QueryError> {
    if let [KeyPart::Float64(a), KeyPart::Float64(b), KeyPart::Float64(c), KeyPart::Float64(d)] =
        key.0.as_slice()
    {
        let m = Mbr::new(
            f64_from_order_u64(*a),
            f64_from_order_u64(*b),
            f64_from_order_u64(*c),
            f64_from_order_u64(*d),
        );
        validate_mbr(m)?;
        Ok(m)
    } else {
        Err(corrupt("invalid spatial key"))
    }
}
fn key(m: Mbr) -> IndexKey {
    IndexKey::from(
        [m.minx, m.miny, m.maxx, m.maxy]
            .into_iter()
            .map(|v| KeyPart::Float64(f64_to_order_u64(v)))
            .collect::<Vec<_>>(),
    )
}
fn pack(row: RowId) -> u64 {
    ((row.page_id as u64) << 16) | row.slot_id as u64
}
fn unpack(v: u64) -> RowId {
    RowId {
        page_id: (v >> 16) as u32,
        slot_id: v as u16,
    }
}

#[derive(Debug)]
pub(crate) struct SpatialCandidates {
    rows: Vec<RowId>,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}
impl std::ops::Deref for SpatialCandidates {
    type Target = Vec<RowId>;
    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}
impl std::ops::DerefMut for SpatialCandidates {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rows
    }
}
impl PartialEq<Vec<RowId>> for SpatialCandidates {
    fn eq(&self, rhs: &Vec<RowId>) -> bool {
        &self.rows == rhs
    }
}
impl SpatialCandidates {
    pub(crate) fn extend_checked(
        &mut self,
        rows: impl IntoIterator<Item = RowId>,
    ) -> Result<(), QueryError> {
        for row in rows {
            if self.rows.len() == self.rows.capacity() {
                let extra = self.rows.capacity().max(64);
                self.charge.try_grow(extra * std::mem::size_of::<RowId>())?;
                self.rows.reserve_exact(extra);
            }
            self.rows.push(row);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct SpatialIndexFile {
    path: PathBuf,
    rel_path: String,
    index_id: u64,
    pool: Arc<BufferPool>,
    file_key: Arc<BufferPageFileKey>,
    log: Arc<crate::wal::LogMutex>,
    durability: DurabilityMode,
    structure: parking_lot::RwLock<()>,
    leaf_latches: Box<[parking_lot::Mutex<()>]>,
    failed: AtomicBool,
    next_page: AtomicU64,
}
impl SpatialIndexFile {
    pub(super) fn open(
        path: PathBuf,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        pool: Arc<BufferPool>,
        create: bool,
    ) -> Result<Self, QueryError> {
        let file_key = Arc::new(BufferPool::index_file_key(path.clone()));
        let this = Self {
            rel_path: IndexFile::compute_relative_path(&path),
            index_id: persistent_index_id(&path),
            path,
            pool,
            file_key,
            log,
            durability,
            structure: parking_lot::RwLock::new(()),
            leaf_latches: (0..256).map(|_| parking_lot::Mutex::new(())).collect(),
            failed: AtomicBool::new(false),
            next_page: AtomicU64::new(2),
        };
        if !create && this.path.with_extension("rtree.failed").exists() {
            return Err(corrupt(
                "incomplete page publication; WAL recovery required",
            ));
        }
        if !create && this.path.with_extension("rtree.corrupt").exists() {
            return Err(corrupt("persistent corruption requires explicit repair"));
        }
        if create {
            this.pool.discard_file(&this.file_key)?;
            if let Some(parent) = this.path.parent() {
                fs::create_dir_all(parent).map_err(io_err)?;
            }
            for path in [
                this.path.clone(),
                this.path.with_extension("rtree.corrupt"),
                this.path.with_extension("rtree.failed"),
            ] {
                match fs::remove_file(path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(io_err(e)),
                }
            }
            let mut pages = BTreeMap::new();
            pages.insert(
                0,
                Meta {
                    root: 1,
                    next: 2,
                    free: 0,
                    index_id: this.index_id,
                    height: 1,
                }
                .encode(),
            );
            pages.insert(
                1,
                Node {
                    level: 0,
                    items: Vec::new(),
                }
                .encode()?,
            );
            this.publish(pages, 0, &[])?;
        } else {
            let meta = this.meta()?;
            this.next_page.store(meta.next, Ordering::Release);
            if this.node(meta.root)?.level + 1 != meta.height {
                return Err(corrupt("root height mismatch"));
            }
        }
        Ok(this)
    }
    pub(super) fn healthy(&self) -> Result<(), QueryError> {
        if self.failed.load(Ordering::Acquire) {
            Err(corrupt("failed page publication; restart required"))
        } else {
            Ok(())
        }
    }
    fn page_key(&self, id: u64) -> PageKey {
        PageKey {
            file: self.file_key.clone(),
            page_id: id,
        }
    }
    fn read(&self, id: u64) -> Result<Vec<u8>, QueryError> {
        self.pool.read_raw_page(self.page_key(id))
    }
    fn meta(&self) -> Result<Meta, QueryError> {
        self.pool.with_raw_page(self.page_key(0), |bytes| {
            let meta = Meta::decode(bytes)?;
            if meta.index_id != self.index_id {
                return Err(corrupt("index identity mismatch"));
            }
            Ok(meta)
        })
    }
    fn node(&self, id: u64) -> Result<Node, QueryError> {
        Node::decode(&self.read(id)?)
    }
    fn publish(
        &self,
        mut pages: BTreeMap<u64, Vec<u8>>,
        txn_id: u64,
        changes: &[(WalIndexOpKind, Item)],
    ) -> Result<(), QueryError> {
        for (&id, image) in &mut pages {
            stamp_page_owner(image, &self.file_key.owner, id);
            refresh_page_checksum(image);
        }
        if pages.len() > MAX_BATCH_PAGES {
            return Err(corrupt("structural batch exceeds page limit"));
        }
        let _charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            pages.len() * PAGE_SIZE * 3,
        )?;
        let deleted = changes
            .iter()
            .find_map(|(kind, item)| (*kind == WalIndexOpKind::Delete).then_some(*item));
        let lsn = if self.durability == DurabilityMode::Memory {
            0
        } else {
            let mut bytes = Vec::with_capacity(53 + pages.len() * (8 + PAGE_SIZE));
            bytes.extend_from_slice(&txn_id.to_le_bytes());
            bytes.push(u8::from(deleted.is_some()));
            let item = deleted.unwrap_or(Item {
                mbr: Mbr::new(0.0, 0.0, 0.0, 0.0),
                pointer: 0,
            });
            for v in [item.mbr.minx, item.mbr.miny, item.mbr.maxx, item.mbr.maxy] {
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            bytes.extend_from_slice(&item.pointer.to_le_bytes());
            bytes.extend_from_slice(&(pages.len() as u32).to_le_bytes());
            for (id, image) in &pages {
                bytes.extend_from_slice(&id.to_le_bytes());
                bytes.extend_from_slice(image);
            }
            let record = crate::wal::LogManager::pre_encode_record(
                0,
                WalPayload::SpatialPageBatch {
                    path: self.rel_path.clone(),
                    bytes: lz4_flex::compress_prepend_size(&bytes),
                },
            )?;
            let undo = if txn_id != 0 && !changes.is_empty() {
                Some(LogManager::pre_encode_record(
                    txn_id,
                    WalPayload::SpatialIndexUndo {
                        path: self.rel_path.clone(),
                        bytes: encode_inverse(changes)?,
                    },
                )?)
            } else {
                None
            };
            let mut log = self.log.lock();
            if let Some(undo) = undo {
                log.append_pre_encoded(undo)?;
            }
            log.append_pre_encoded(record)?
        };
        let next = pages.get(&0).map(|p| read_u64(p, 72));
        // The complete batch is in WAL before any page becomes evictable.
        #[cfg(test)]
        let inject_crash = !CRASH_STRUCTURAL_ONLY.with(|flag| flag.get())
            || (pages.contains_key(&0) && pages.len() >= 3);
        #[cfg(test)]
        if inject_crash {
            crash_point(&self.log, lsn);
        }
        for (id, image) in &mut pages {
            set_page_lsn_if_formatted(image, lsn);
            if let Err(err) = self
                .pool
                .store_page(self.page_key(*id), std::mem::take(image), lsn)
            {
                self.fail_publication(lsn);
                return Err(err);
            }
            #[cfg(test)]
            if inject_crash {
                crash_point(&self.log, lsn);
            }
        }
        if let Some(next) = next {
            self.next_page.store(next, Ordering::Release);
        }
        Ok(())
    }
    fn fail_publication(&self, lsn: u64) {
        self.failed.store(true, Ordering::Release);
        if self.durability != DurabilityMode::Memory {
            let marker = self.path.with_extension("rtree.failed");
            if let Ok(mut file) = File::create(&marker) {
                let _ = file.write_all(&lsn.to_le_bytes());
                let _ = file.sync_all();
            }
        }
    }
    pub(super) fn flush(&self) -> Result<(), QueryError> {
        self.healthy()?;
        if self.durability != DurabilityMode::Memory
            || crate::storage::temp_quota::is_temporary_path(&self.path)
        {
            self.pool.flush_file(&self.file_key)?;
        }
        Ok(())
    }
    /// Bounded sort-tile-recursive packing. The existing external sorter supplies
    /// X-ordered leaf keys; tiles are Y-ordered and upper levels spill descriptors.
    pub(super) fn build_sorted(
        &self,
        mut entries: impl Iterator<Item = Result<IndexEntry, QueryError>>,
    ) -> Result<(), QueryError> {
        let _structure = self.structure.write();
        let root = self
            .path
            .ancestors()
            .nth(Path::new(&self.rel_path).components().count())
            .ok_or_else(|| corrupt("invalid object path"))?;
        let tmp = IndexSortTmpDir::create(root)?;
        let tile_size = CAPACITY * 16;
        let _charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            tile_size * std::mem::size_of::<Item>() * 3 + PAGE_SIZE * 40,
        )?;
        let mut level = 0u16;
        let mut next_page = 1u64;
        let mut previous: Option<(std::io::BufReader<File>, usize)> = None;
        let mut last_key: Option<(IndexKey, RowId)> = None;
        loop {
            let output_path = tmp.path.join(format!("spatial-level-{level}"));
            let mut output = std::io::BufWriter::new(File::create(&output_path).map_err(io_err)?);
            let mut output_count = 0usize;
            let mut pages = BTreeMap::new();
            let mut last_page = 0;
            loop {
                let mut tile = Vec::with_capacity(tile_size);
                while tile.len() < tile_size {
                    let item = if let Some((reader, remaining)) = previous.as_mut() {
                        if *remaining == 0 {
                            None
                        } else {
                            let mut bytes = [0u8; 40];
                            reader.read_exact(&mut bytes).map_err(io_err)?;
                            *remaining -= 1;
                            Some(Item {
                                mbr: Mbr::new(
                                    f64::from_bits(read_u64(&bytes, 0)),
                                    f64::from_bits(read_u64(&bytes, 8)),
                                    f64::from_bits(read_u64(&bytes, 16)),
                                    f64::from_bits(read_u64(&bytes, 24)),
                                ),
                                pointer: read_u64(&bytes, 32),
                            })
                        }
                    } else {
                        loop {
                            let Some(e) = entries.next() else {
                                break None;
                            };
                            let e = e?;
                            if last_key
                                .as_ref()
                                .is_some_and(|(k, r)| k == &e.key && *r == e.row_id)
                            {
                                continue;
                            }
                            let item = Item {
                                mbr: decode_key(&e.key)?,
                                pointer: pack(e.row_id),
                            };
                            last_key = Some((e.key, e.row_id));
                            break Some(item);
                        }
                    };
                    match item {
                        Some(e) => tile.push(e),
                        None => break,
                    }
                }
                if tile.is_empty() {
                    break;
                }
                tile.sort_by(|a, b| {
                    a.mbr
                        .miny
                        .total_cmp(&b.mbr.miny)
                        .then_with(|| a.mbr.minx.total_cmp(&b.mbr.minx))
                        .then_with(|| a.pointer.cmp(&b.pointer))
                });
                // Distribute the final tile evenly instead of leaving a nearly empty page.
                let page_count = tile.len().div_ceil(CAPACITY);
                let per_page = tile.len().div_ceil(page_count);
                for chunk in tile.chunks(per_page) {
                    let node = Node {
                        level,
                        items: chunk.to_vec(),
                    };
                    let cover = node.cover().ok_or_else(|| corrupt("empty bulk page"))?;
                    let id = next_page;
                    next_page = next_page
                        .checked_add(1)
                        .ok_or_else(|| corrupt("page id overflow"))?;
                    last_page = id;
                    pages.insert(id, node.encode()?);
                    output_count += 1;
                    for v in [cover.minx, cover.miny, cover.maxx, cover.maxy] {
                        output.write_all(&v.to_le_bytes()).map_err(io_err)?;
                    }
                    output.write_all(&id.to_le_bytes()).map_err(io_err)?;
                    if pages.len() == 16 {
                        self.publish(std::mem::take(&mut pages), 0, &[])?;
                    }
                }
                crate::engine::cancel::check_cancelled()?;
            }
            output.flush().map_err(io_err)?;
            drop(output);
            if !pages.is_empty() {
                self.publish(pages, 0, &[])?;
            }
            if output_count == 0 {
                return Ok(());
            }
            if output_count == 1 {
                self.publish(
                    BTreeMap::from([(
                        0,
                        Meta {
                            root: last_page,
                            next: next_page,
                            free: 0,
                            index_id: self.index_id,
                            height: level + 1,
                        }
                        .encode(),
                    )]),
                    0,
                    &[],
                )?;
                return Ok(());
            }
            previous = Some((
                std::io::BufReader::new(File::open(output_path).map_err(io_err)?),
                output_count,
            ));
            level = level
                .checked_add(1)
                .ok_or_else(|| corrupt("tree too deep"))?;
        }
    }

    pub(super) fn count_pages(&self) -> u64 {
        self.next_page.load(Ordering::Acquire)
    }
    pub(super) fn search(&self, query: Mbr) -> Result<SpatialCandidates, QueryError> {
        self.search_impl(query, false)
    }
    pub(super) fn lookup(&self, key: &IndexKey) -> Result<Vec<RowId>, QueryError> {
        Ok(self.search_impl(decode_key(key)?, true)?.rows)
    }
    pub(super) fn is_empty(&self) -> Result<bool, QueryError> {
        let _guard = self.structure.read();
        Ok(self.node(self.meta()?.root)?.items.is_empty())
    }
    fn search_impl(&self, query: Mbr, exact: bool) -> Result<SpatialCandidates, QueryError> {
        self.healthy()?;
        validate_mbr(query)?;
        let _guard = self.structure.read();
        let meta = self.meta()?;
        let mut stack = vec![(meta.root, None)];
        let mut out = Vec::new();
        let mut charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            PAGE_SIZE * 4,
        )?;
        while let Some((id, expected_level)) = stack.pop() {
            crate::engine::cancel::check_cancelled()?;
            if id == 0 || id >= meta.next {
                return Err(corrupt("invalid child id"));
            }
            self.pool.with_raw_page(self.page_key(id), |bytes| {
                validate_page(bytes)?;
                let level = read_u16(bytes, 26);
                let count = read_u16(bytes, 28) as usize;
                if bytes[24] != 1
                    || count > CAPACITY
                    || level > 64
                    || expected_level.is_some_and(|expected| expected != level)
                {
                    return Err(corrupt("invalid tree page or level"));
                }
                for i in 0..count {
                    let p = HEADER + i * 40;
                    let m = Mbr::new(
                        f64::from_bits(read_u64(bytes, p)),
                        f64::from_bits(read_u64(bytes, p + 8)),
                        f64::from_bits(read_u64(bytes, p + 16)),
                        f64::from_bits(read_u64(bytes, p + 24)),
                    );
                    validate_mbr(m)?;
                    if !m.intersects(&query) {
                        continue;
                    }
                    let pointer = read_u64(bytes, p + 32);
                    if level == 0 {
                        if pointer >> 48 != 0 {
                            return Err(corrupt("invalid row id"));
                        }
                        if exact && !m.equals(&query) {
                            continue;
                        }
                        if out.len() == out.capacity() {
                            let extra = out.capacity().max(64);
                            charge.try_grow(extra * std::mem::size_of::<RowId>())?;
                            out.reserve_exact(extra);
                        }
                        out.push(unpack(pointer));
                    } else {
                        if stack.len() == stack.capacity() {
                            let extra = stack.capacity().max(64);
                            charge.try_grow(extra * std::mem::size_of::<(u64, Option<u16>)>())?;
                            stack.reserve_exact(extra);
                        }
                        stack.push((pointer, Some(level - 1)));
                    }
                }
                Ok(())
            })?;
        }
        Ok(SpatialCandidates { rows: out, charge })
    }
    pub(super) fn entries(&self) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        self.healthy()?;
        let _guard = self.structure.read();
        let meta = self.meta()?;
        let mut stack = vec![meta.root];
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        while let Some(id) = stack.pop() {
            if id == 0 || id >= meta.next || !seen.insert(id) {
                return Err(corrupt("invalid child graph"));
            }
            let n = self.node(id)?;
            for e in n.items {
                if n.level == 0 {
                    out.push((key(e.mbr), unpack(e.pointer)));
                } else {
                    stack.push(e.pointer);
                }
            }
        }
        Ok(out)
    }
    pub(super) fn insert_many(
        &self,
        txn_id: u64,
        entries: Vec<(IndexKey, RowId)>,
    ) -> Result<(), QueryError> {
        self.healthy()?;
        for chunk in entries.chunks(16) {
            let _guard = self.structure.write();
            let mut edit = Edit::new(self)?;
            let mut changes = Vec::with_capacity(chunk.len());
            for (key, row) in chunk {
                let item = Item {
                    mbr: decode_key(key)?,
                    pointer: pack(*row),
                };
                if edit
                    .locate(edit.meta.root, item, &mut Vec::new())?
                    .is_some()
                {
                    continue;
                }
                changes.push((WalIndexOpKind::Insert, item));
                let root = edit.meta.root;
                let level = edit.node(root)?.level;
                if let Some(sibling) = edit.insert(root, item)? {
                    let left = edit
                        .node(root)?
                        .cover()
                        .ok_or_else(|| corrupt("empty root"))?;
                    let right = edit
                        .node(sibling)?
                        .cover()
                        .ok_or_else(|| corrupt("empty sibling"))?;
                    let id = edit.allocate()?;
                    edit.put(
                        id,
                        Node {
                            level: level + 1,
                            items: vec![
                                Item {
                                    mbr: left,
                                    pointer: root,
                                },
                                Item {
                                    mbr: right,
                                    pointer: sibling,
                                },
                            ],
                        },
                    )?;
                    edit.meta.root = id;
                }
            }
            edit.meta.height = edit.node(edit.meta.root)?.level + 1;
            if edit.meta != self.meta()? {
                edit.pages.insert(0, edit.meta.encode());
            }
            if !edit.pages.is_empty() {
                self.publish(edit.pages, txn_id, &changes)?;
            }
        }
        Ok(())
    }

    pub(super) fn apply(
        &self,
        txn_id: u64,
        entries: impl IntoIterator<Item = IndexBatchEntry>,
    ) -> Result<(), QueryError> {
        self.healthy()?;
        for entry in entries {
            let mbr = decode_key(&entry.key)?;
            let item = Item {
                mbr,
                pointer: pack(entry.row_id),
            };
            if self.try_leaf_change(txn_id, entry.kind, item)? {
                continue;
            }
            let _guard = self.structure.write();
            let mut edit = Edit::new(self)?;
            match entry.kind {
                WalIndexOpKind::Insert => {
                    if edit
                        .locate(edit.meta.root, item, &mut Vec::new())?
                        .is_some()
                    {
                        continue;
                    }
                    let root = edit.meta.root;
                    let old_level = edit.node(root)?.level;
                    if let Some(sibling) = edit.insert(root, item)? {
                        let left = edit
                            .node(root)?
                            .cover()
                            .ok_or_else(|| corrupt("empty split root"))?;
                        let right = edit
                            .node(sibling)?
                            .cover()
                            .ok_or_else(|| corrupt("empty sibling"))?;
                        let id = edit.allocate()?;
                        edit.put(
                            id,
                            Node {
                                level: old_level + 1,
                                items: vec![
                                    Item {
                                        mbr: left,
                                        pointer: root,
                                    },
                                    Item {
                                        mbr: right,
                                        pointer: sibling,
                                    },
                                ],
                            },
                        )?;
                        edit.meta.root = id;
                    }
                }
                WalIndexOpKind::Delete => {
                    let mut path = Vec::new();
                    let Some((leaf, slot)) = edit.locate(edit.meta.root, item, &mut path)? else {
                        continue;
                    };
                    let mut n = edit.node(leaf)?;
                    n.items.remove(slot);
                    edit.put(leaf, n)?;
                    let mut child = leaf;
                    for (parent, slot) in path.into_iter().rev() {
                        if !edit.node(child)?.items.is_empty() {
                            break;
                        }
                        edit.free(child)?;
                        let mut n = edit.node(parent)?;
                        n.items.remove(slot);
                        edit.put(parent, n)?;
                        child = parent;
                    }
                    loop {
                        let root = edit.meta.root;
                        let n = edit.node(root)?;
                        if n.level > 0 && n.items.len() == 1 {
                            edit.meta.root = n.items[0].pointer;
                            edit.free(root)?;
                        } else {
                            if n.items.is_empty() && n.level > 0 {
                                edit.put(
                                    root,
                                    Node {
                                        level: 0,
                                        items: Vec::new(),
                                    },
                                )?;
                            }
                            break;
                        }
                    }
                }
            }
            edit.meta.height = edit.node(edit.meta.root)?.level + 1;
            if edit.meta != self.meta()? {
                edit.pages.insert(0, edit.meta.encode());
            }
            self.publish(edit.pages, txn_id, &[(entry.kind, item)])?;
        }
        Ok(())
    }
    fn try_leaf_change(
        &self,
        txn_id: u64,
        kind: WalIndexOpKind,
        item: Item,
    ) -> Result<bool, QueryError> {
        let _structure = self.structure.read();
        let edit = Edit::new(self)?;
        let found = edit.locate(edit.meta.root, item, &mut Vec::new())?;
        if kind == WalIndexOpKind::Insert && found.is_some() {
            return Ok(true);
        }
        if kind == WalIndexOpKind::Delete && found.is_none() {
            return Ok(true);
        }
        let id = if let Some((id, _)) = found {
            id
        } else {
            let mut id = edit.meta.root;
            loop {
                let node = self.node(id)?;
                if node.level == 0 {
                    break id;
                }
                let Some(child) = node.items.iter().min_by(|a, b| {
                    enlargement(a.mbr, item.mbr)
                        .total_cmp(&enlargement(b.mbr, item.mbr))
                        .then_with(|| a.mbr.area().total_cmp(&b.mbr.area()))
                        .then_with(|| a.pointer.cmp(&b.pointer))
                }) else {
                    return Err(corrupt("empty internal page"));
                };
                if !child.mbr.contains(&item.mbr) {
                    return Ok(false);
                }
                if self.node(child.pointer)?.level.checked_add(1) != Some(node.level) {
                    return Err(corrupt("unbalanced child"));
                }
                id = child.pointer;
            }
        };
        let _leaf = self.leaf_latches[id as usize % self.leaf_latches.len()].lock();
        let mut node = self.node(id)?;
        match kind {
            WalIndexOpKind::Insert => {
                if node
                    .items
                    .iter()
                    .any(|e| e.pointer == item.pointer && e.mbr.equals(&item.mbr))
                {
                    return Ok(true);
                }
                if node.items.len() == CAPACITY {
                    return Ok(false);
                }
                node.items.push(item);
            }
            WalIndexOpKind::Delete => {
                let Some(slot) = node
                    .items
                    .iter()
                    .position(|e| e.pointer == item.pointer && e.mbr.equals(&item.mbr))
                else {
                    return Ok(true);
                };
                if node.items.len() == 1 && id != edit.meta.root {
                    return Ok(false);
                }
                node.items.remove(slot);
            }
        }
        self.publish(
            BTreeMap::from([(id, node.encode()?)]),
            txn_id,
            &[(kind, item)],
        )?;
        Ok(true)
    }

    pub(super) fn validate(&self) -> Result<(), QueryError> {
        let _guard = self.structure.read();
        let meta = self.meta()?;
        let mut seen = HashSet::new();
        let mut rows = HashSet::new();
        let mut stack = vec![(meta.root, None, None)];
        while let Some((id, level, cover)) = stack.pop() {
            if id == 0 || id >= meta.next || !seen.insert(id) {
                return Err(corrupt("invalid page reachability"));
            }
            let n = self.node(id)?;
            if level.is_some_and(|v| v != n.level) {
                return Err(corrupt("unbalanced levels"));
            }
            if let (Some(parent), Some(child)) = (cover, n.cover()) {
                if !Mbr::contains(&parent, &child) {
                    return Err(corrupt("parent does not cover child"));
                }
            }
            for e in n.items {
                if n.level == 0 {
                    if !rows.insert((key(e.mbr), e.pointer)) {
                        return Err(corrupt("duplicate entry"));
                    }
                } else {
                    stack.push((e.pointer, Some(n.level - 1), Some(e.mbr)));
                }
            }
        }
        let mut free = meta.free;
        while free != 0 {
            if free >= meta.next || !seen.insert(free) {
                return Err(corrupt("invalid free list"));
            }
            let p = self.read(free)?;
            validate_page(&p)?;
            if p[24] != 2 {
                return Err(corrupt("invalid free page"));
            }
            free = read_u64(&p, 32);
        }
        if seen.len() as u64 != meta.next - 1 {
            return Err(corrupt("unreachable pages"));
        }
        Ok(())
    }
    pub(super) fn quick_validate(path: PathBuf) -> Result<(), QueryError> {
        if path.with_extension("rtree.failed").exists() {
            return Err(corrupt(
                "incomplete page publication; WAL recovery required",
            ));
        }
        if path.with_extension("rtree.corrupt").exists() {
            return Err(corrupt("persistent corruption requires explicit repair"));
        }
        let index_id = persistent_index_id(&path);
        let mut file = File::open(path).map_err(io_err)?;
        let meta = Meta::decode(&read_index_page(&mut file, 0)?)?;
        if meta.index_id != index_id {
            return Err(corrupt("index identity mismatch"));
        }
        if Node::decode(&read_index_page(&mut file, meta.root)?)?.level + 1 != meta.height {
            return Err(corrupt("root height mismatch"));
        }
        Ok(())
    }
}
struct Edit<'a> {
    tree: &'a SpatialIndexFile,
    meta: Meta,
    pages: BTreeMap<u64, Vec<u8>>,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}
impl<'a> Edit<'a> {
    fn new(tree: &'a SpatialIndexFile) -> Result<Self, QueryError> {
        Ok(Self {
            tree,
            meta: tree.meta()?,
            pages: BTreeMap::new(),
            charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                PAGE_SIZE * 4,
            )?,
        })
    }
    fn node(&self, id: u64) -> Result<Node, QueryError> {
        if id == 0 || id >= self.meta.next {
            return Err(corrupt("invalid child id"));
        }
        match self.pages.get(&id) {
            Some(p) => Node::decode(p),
            None => self.tree.node(id),
        }
    }
    fn put(&mut self, id: u64, n: Node) -> Result<(), QueryError> {
        if self.pages.len() >= MAX_BATCH_PAGES {
            return Err(corrupt("tree too deep"));
        }
        if !self.pages.contains_key(&id) {
            self.charge.try_grow(PAGE_SIZE)?;
        }
        self.pages.insert(id, n.encode()?);
        Ok(())
    }
    fn allocate(&mut self) -> Result<u64, QueryError> {
        if self.meta.free != 0 {
            let id = self.meta.free;
            let p = match self.pages.get(&id) {
                Some(p) => p.clone(),
                None => self.tree.read(id)?,
            };
            validate_page(&p)?;
            if p[24] != 2 {
                return Err(corrupt("invalid free page"));
            }
            self.meta.free = read_u64(&p, 32);
            Ok(id)
        } else {
            let id = self.meta.next;
            self.meta.next = self
                .meta
                .next
                .checked_add(1)
                .ok_or_else(|| corrupt("page id overflow"))?;
            Ok(id)
        }
    }
    fn free(&mut self, id: u64) -> Result<(), QueryError> {
        let mut p = page(2);
        write_u64(&mut p, 32, self.meta.free);
        refresh_page_checksum(&mut p);
        self.pages.insert(id, p);
        self.meta.free = id;
        Ok(())
    }
    fn locate(
        &self,
        id: u64,
        item: Item,
        path: &mut Vec<(u64, usize)>,
    ) -> Result<Option<(u64, usize)>, QueryError> {
        if path.len() > 64 {
            return Err(corrupt("cyclic tree"));
        }
        let n = self.node(id)?;
        if n.level == 0 {
            return Ok(n
                .items
                .iter()
                .position(|e| e.pointer == item.pointer && e.mbr.equals(&item.mbr))
                .map(|i| (id, i)));
        }
        for (i, e) in n.items.iter().enumerate() {
            if e.mbr.contains(&item.mbr) {
                path.push((id, i));
                if let Some(found) = self.locate(e.pointer, item, path)? {
                    return Ok(Some(found));
                }
                path.pop();
            }
        }
        Ok(None)
    }
    fn insert(&mut self, id: u64, item: Item) -> Result<Option<u64>, QueryError> {
        let mut n = self.node(id)?;
        if n.level > 64 {
            return Err(corrupt("tree too deep"));
        }
        if n.level == 0 {
            n.items.push(item);
        } else {
            let slot = n
                .items
                .iter()
                .enumerate()
                .min_by(|(_, a), (_, b)| {
                    enlargement(a.mbr, item.mbr)
                        .total_cmp(&enlargement(b.mbr, item.mbr))
                        .then_with(|| a.mbr.area().total_cmp(&b.mbr.area()))
                        .then_with(|| a.pointer.cmp(&b.pointer))
                })
                .map(|(i, _)| i)
                .ok_or_else(|| corrupt("empty internal page"))?;
            let child = n.items[slot].pointer;
            if self.node(child)?.level.checked_add(1) != Some(n.level) {
                return Err(corrupt("unbalanced child"));
            }
            let split = self.insert(child, item)?;
            let cover = self
                .node(child)?
                .cover()
                .ok_or_else(|| corrupt("empty child"))?;
            if split.is_none() && n.items[slot].mbr.equals(&cover) {
                return Ok(None);
            }
            n.items[slot].mbr = cover;
            if let Some(sibling) = split {
                n.items.push(Item {
                    mbr: self
                        .node(sibling)?
                        .cover()
                        .ok_or_else(|| corrupt("empty sibling"))?,
                    pointer: sibling,
                });
            }
        }
        if n.items.len() <= CAPACITY {
            self.put(id, n)?;
            return Ok(None);
        }
        let (a, b) = split(n.items);
        let sibling = self.allocate()?;
        self.put(
            id,
            Node {
                level: n.level,
                items: a,
            },
        )?;
        self.put(
            sibling,
            Node {
                level: n.level,
                items: b,
            },
        )?;
        Ok(Some(sibling))
    }
}
fn enlargement(a: Mbr, b: Mbr) -> f64 {
    let v = a.enlargement(&b);
    if v.is_nan() {
        f64::INFINITY
    } else {
        v
    }
}
fn split(mut items: Vec<Item>) -> (Vec<Item>, Vec<Item>) {
    let mut seeds = (0, 1);
    let mut waste = f64::NEG_INFINITY;
    for a in 0..items.len() {
        for b in a + 1..items.len() {
            let w = items[a].mbr.union(&items[b].mbr).area()
                - items[a].mbr.area()
                - items[b].mbr.area();
            if w.total_cmp(&waste).is_gt() {
                waste = w;
                seeds = (a, b);
            }
        }
    }
    let second = items.remove(seeds.1);
    let first = items.remove(seeds.0);
    let mut a = vec![first];
    let mut b = vec![second];
    let mut ac = first.mbr;
    let mut bc = second.mbr;
    while !items.is_empty() {
        if a.len() + items.len() == MIN_FILL {
            a.extend(items);
            break;
        }
        if b.len() + items.len() == MIN_FILL {
            b.extend(items);
            break;
        }
        let i = items
            .iter()
            .enumerate()
            .max_by(|(_, x), (_, y)| {
                let d = |e: &Item| (enlargement(ac, e.mbr) - enlargement(bc, e.mbr)).abs();
                d(x).total_cmp(&d(y))
            })
            .map(|(i, _)| i)
            .unwrap();
        let e = items.remove(i);
        let order = enlargement(ac, e.mbr)
            .total_cmp(&enlargement(bc, e.mbr))
            .then_with(|| ac.area().total_cmp(&bc.area()))
            .then_with(|| a.len().cmp(&b.len()));
        if order.is_le() {
            ac = ac.union(&e.mbr);
            a.push(e);
        } else {
            bc = bc.union(&e.mbr);
            b.push(e);
        }
    }
    (a, b)
}

// The inverse is linked into the user's WAL chain BEFORE publishing redo-only
// pages. Native recovery has no catalog and cannot rely on heap-derived repair.
fn encode_inverse(changes: &[(WalIndexOpKind, Item)]) -> Result<Vec<u8>, QueryError> {
    if changes.is_empty() || changes.len() > 16 {
        return Err(corrupt("invalid spatial undo batch"));
    }
    let mut bytes = Vec::with_capacity(4 + changes.len() * 41);
    bytes.extend_from_slice(&(changes.len() as u32).to_le_bytes());
    for (kind, item) in changes.iter().rev() {
        bytes.push(if *kind == WalIndexOpKind::Insert {
            2
        } else {
            1
        });
        for v in [item.mbr.minx, item.mbr.miny, item.mbr.maxx, item.mbr.maxy] {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        bytes.extend_from_slice(&item.pointer.to_le_bytes());
    }
    Ok(bytes)
}
pub(super) fn decode_inverse(bytes: &[u8]) -> Result<Vec<IndexBatchEntry>, QueryError> {
    if bytes.len() < 4 {
        return Err(corrupt("truncated spatial undo"));
    }
    let count = read_u32(bytes, 0) as usize;
    if count == 0 || count > 16 || bytes.len() != 4 + count * 41 {
        return Err(corrupt("invalid spatial undo length"));
    }
    let mut out = Vec::with_capacity(count);
    for entry in bytes[4..].chunks_exact(41) {
        let kind = match entry[0] {
            1 => WalIndexOpKind::Insert,
            2 => WalIndexOpKind::Delete,
            _ => return Err(corrupt("invalid spatial undo kind")),
        };
        let m = Mbr::new(
            f64::from_bits(read_u64(entry, 1)),
            f64::from_bits(read_u64(entry, 9)),
            f64::from_bits(read_u64(entry, 17)),
            f64::from_bits(read_u64(entry, 25)),
        );
        validate_mbr(m)?;
        let row = read_u64(entry, 33);
        if row >> 48 != 0 {
            return Err(corrupt("invalid spatial undo row"));
        }
        out.push(IndexBatchEntry {
            kind,
            key: key(m),
            row_id: unpack(row),
        });
    }
    Ok(out)
}

/// Own the decompressed image and its reservation through the last applied page.
struct DecodedBatch {
    bytes: Vec<u8>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}
impl DecodedBatch {
    fn pages(&self) -> impl Iterator<Item = (u64, &[u8])> {
        self.bytes[53..]
            .chunks_exact(8 + PAGE_SIZE)
            .map(|entry| (read_u64(entry, 0), &entry[8..]))
    }
}

/// Validate the entire batch before allowing a prefix to reach disk.
fn batch_pages(encoded: &[u8]) -> Result<DecodedBatch, QueryError> {
    if encoded.len() < 4 {
        return Err(corrupt("truncated compressed WAL batch"));
    }
    let length = read_u32(encoded, 0) as usize;
    if length < 53 || length > 53 + MAX_BATCH_PAGES * (8 + PAGE_SIZE) {
        return Err(corrupt("invalid compressed WAL batch"));
    }
    let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
        length + MAX_BATCH_PAGES * 64,
    )?;
    let decoded = lz4_flex::decompress_size_prepended(encoded)
        .map_err(|_| corrupt("invalid compressed WAL batch"))?;
    let count = read_u32(&decoded, 49) as usize;
    if count == 0 || count > MAX_BATCH_PAGES || decoded.len() != 53 + count * (8 + PAGE_SIZE) {
        return Err(corrupt("invalid WAL batch length"));
    }
    let batch = DecodedBatch {
        bytes: decoded,
        _charge: charge,
    };
    let mut seen = HashSet::new();
    for (id, image) in batch.pages() {
        if !seen.insert(id) {
            return Err(corrupt("duplicate WAL page"));
        }
        validate_page(image)?;
    }
    Ok(batch)
}
pub(super) fn redo(data_dir: &Path, path: &str, bytes: &[u8], lsn: u64) -> Result<(), QueryError> {
    if !path.ends_with(".rtree")
        || path.split('/').any(|p| p == "..")
        || Path::new(path).is_absolute()
    {
        return Err(corrupt("invalid WAL path"));
    }
    let batch = batch_pages(bytes)?;
    for (id, image) in batch.pages() {
        redo_page_update(data_dir, path, id, lsn, image)?;
    }
    let marker = data_dir.join(path).with_extension("rtree.failed");
    if let Ok(bytes) = fs::read(&marker) {
        if bytes.len() == 8 && read_u64(&bytes, 0) <= lsn {
            fs::remove_file(marker).map_err(io_err)?;
        }
    }
    Ok(())
}

/// Old keys must be registered on replicas before publishing their removal.
pub(super) fn deleted_key(bytes: &[u8]) -> Result<Option<(u64, IndexKey, RowId)>, QueryError> {
    let batch = batch_pages(bytes)?;
    let bytes = batch.bytes.as_slice();
    if bytes[8] == 0 {
        return Ok(None);
    }
    if bytes[8] != 1 {
        return Err(corrupt("invalid WAL operation"));
    }
    let m = Mbr::new(
        f64::from_bits(read_u64(bytes, 9)),
        f64::from_bits(read_u64(bytes, 17)),
        f64::from_bits(read_u64(bytes, 25)),
        f64::from_bits(read_u64(bytes, 33)),
    );
    validate_mbr(m)?;
    Ok(Some((
        read_u64(bytes, 0),
        key(m),
        unpack(read_u64(bytes, 41)),
    )))
}
impl SpatialIndexFile {
    pub(super) fn replay(&self, data_dir: &Path, bytes: &[u8], lsn: u64) -> Result<(), QueryError> {
        let _guard = self.structure.write();
        // No cached metadata lives outside the buffer pool. Hold the structure
        // gate until every page has been applied and stale frames discarded.
        if let Err(err) = redo(data_dir, &self.rel_path, bytes, lsn)
            .and_then(|_| self.pool.discard_pages_for_path(&self.path))
        {
            self.fail_publication(lsn);
            return Err(err);
        }
        self.next_page.store(self.meta()?.next, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn release_format_golden_spatial_pages() {
        use crate::storage::format_golden::golden;
        let mut meta = Meta {
            root: 1,
            next: 2,
            free: 0,
            index_id: 17,
            height: 1,
        }
        .encode();
        stamp_page_owner(&mut meta, &[0xa5; 16], 0);
        set_page_lsn_if_formatted(&mut meta, 17);
        golden!("spatial-meta", &meta);
        let mut node = Node {
            level: 0,
            items: Vec::new(),
        }
        .encode()
        .unwrap();
        stamp_page_owner(&mut node, &[0xa5; 16], 1);
        set_page_lsn_if_formatted(&mut node, 17);
        golden!("spatial-page", &node);
    }

    use super::*;
    fn directory() -> tempfile::TempDir {
        fs::create_dir_all("/tmp/mydb").unwrap();
        tempfile::tempdir_in("/tmp/mydb").unwrap()
    }
    fn open(dir: &Path, create: bool) -> SpatialIndexFile {
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir).unwrap()));
        let pool = BufferPool::new(
            128,
            log.clone(),
            DurabilityMode::Strict,
            Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
        );
        SpatialIndexFile::open(
            dir.join("objects/indexes/99/1/1.rtree"),
            log,
            DurabilityMode::Strict,
            pool,
            create,
        )
        .unwrap()
    }
    fn entry(i: u32, kind: WalIndexOpKind) -> IndexBatchEntry {
        let x = ((i as u64 * 7919) % 1009) as f64;
        let y = ((i as u64 * 104729) % 1013) as f64;
        IndexBatchEntry {
            kind,
            key: key(Mbr::new(x, y, x + (i % 7) as f64, y + (i % 11) as f64)),
            row_id: RowId {
                page_id: i / 100,
                slot_id: (i % 100) as u16,
            },
        }
    }
    #[test]
    fn spatial_index_persisted_split_delete_reuse_and_reopen() {
        let dir = directory();
        let tree = open(dir.path(), true);
        for i in 0..1800 {
            tree.apply(0, [entry(i, WalIndexOpKind::Insert)]).unwrap();
        }
        tree.validate().unwrap();
        for q in 0..40 {
            let window = Mbr::new(
                (q * 19) as f64,
                (q * 13) as f64,
                (q * 19 + 80) as f64,
                (q * 13 + 100) as f64,
            );
            let mut expected = (0..1800)
                .filter_map(|i| {
                    let e = entry(i, WalIndexOpKind::Insert);
                    decode_key(&e.key)
                        .unwrap()
                        .intersects(&window)
                        .then_some(e.row_id)
                })
                .collect::<Vec<_>>();
            let mut actual = tree.search(window).unwrap();
            expected.sort();
            actual.sort();
            assert_eq!(actual, expected);
        }
        tree.flush().unwrap();
        drop(tree);
        let tree = open(dir.path(), false);
        assert_eq!(tree.entries().unwrap().len(), 1800);
        let pages = tree.count_pages();
        for i in 0..1800 {
            tree.apply(0, [entry(i, WalIndexOpKind::Delete)]).unwrap();
        }
        tree.validate().unwrap();
        assert!(tree.entries().unwrap().is_empty());
        for i in 0..1800 {
            tree.apply(
                0,
                [
                    entry(i, WalIndexOpKind::Insert),
                    entry(i, WalIndexOpKind::Insert),
                ],
            )
            .unwrap();
        }
        assert_eq!(tree.entries().unwrap().len(), 1800);
        assert_eq!(tree.count_pages(), pages);
        tree.validate().unwrap();
    }
    #[test]
    fn spatial_index_wal_replay_is_idempotent_and_repairs_torn_pages() {
        let source = directory();
        let target = directory();
        let tree = open(source.path(), true);
        for i in 0..700 {
            tree.apply(0, [entry(i, WalIndexOpKind::Insert)]).unwrap();
        }
        let last = tree.log.lock().last_lsn();
        LogManager::sync_durable_to(&tree.log, last).unwrap();
        let scan = LogManager::scan(source.path()).unwrap();
        fs::create_dir_all(target.path().join("system")).unwrap();
        fs::copy(
            source.path().join(crate::storage::control::PATH),
            target.path().join(crate::storage::control::PATH),
        )
        .unwrap();
        for record in &scan.records {
            if let WalPayload::SpatialPageBatch { path, bytes } = &record.payload {
                redo(target.path(), path, bytes, record.lsn).unwrap();
                redo(target.path(), path, bytes, record.lsn).unwrap();
            }
        }
        let replica = open(target.path(), false);
        replica.validate().unwrap();
        assert_eq!(replica.entries().unwrap().len(), 700);
        drop(replica);
        let path = target.path().join("objects/indexes/99/1/1.rtree");
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.write_all(&vec![0x55; PAGE_SIZE]).unwrap();
        drop(file);
        for record in &scan.records {
            if let WalPayload::SpatialPageBatch { path, bytes } = &record.payload {
                redo(target.path(), path, bytes, record.lsn).unwrap();
            }
        }
        let repaired = open(target.path(), false);
        repaired.validate().unwrap();
        assert_eq!(repaired.entries().unwrap().len(), 700);
        let record = scan
            .records
            .iter()
            .find_map(|r| {
                if let WalPayload::SpatialPageBatch { bytes, .. } = &r.payload {
                    Some(bytes)
                } else {
                    None
                }
            })
            .unwrap();
        assert!(batch_pages(&record[..record.len() - 1]).is_err());
    }
    #[test]
    fn spatial_index_concurrent_leaf_writes_and_queries() {
        let dir = directory();
        let tree = Arc::new(open(dir.path(), true));
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let tree = tree.clone();
                scope.spawn(move || {
                    for i in worker * 400..worker * 400 + 400 {
                        let e = entry(i, WalIndexOpKind::Insert);
                        let mbr = decode_key(&e.key).unwrap();
                        let row = e.row_id;
                        tree.apply(0, [e]).unwrap();
                        if i % 17 == 0 {
                            assert!(tree.search(mbr).unwrap().contains(&row));
                        }
                    }
                });
            }
        });
        tree.validate().unwrap();
        assert_eq!(tree.entries().unwrap().len(), 1600);
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let tree = tree.clone();
                scope.spawn(move || {
                    for i in (worker * 400..worker * 400 + 400).step_by(2) {
                        tree.apply(0, [entry(i, WalIndexOpKind::Delete)]).unwrap();
                    }
                });
            }
        });
        tree.validate().unwrap();
        assert_eq!(tree.entries().unwrap().len(), 800);
    }

    #[test]
    fn spatial_index_bulk_tree_exceeds_small_buffer_pool() {
        let dir = directory();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new(
            8,
            log.clone(),
            DurabilityMode::Strict,
            Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
        );
        let path = dir.path().join("objects/indexes/99/1/1.rtree");
        let tree =
            SpatialIndexFile::open(path.clone(), log, DurabilityMode::Strict, pool, true).unwrap();
        let mut entries = (0..12000)
            .map(|i| {
                let e = entry(i, WalIndexOpKind::Insert);
                IndexEntry {
                    key: e.key,
                    row_id: e.row_id,
                }
            })
            .collect::<Vec<_>>();
        entries.sort();
        tree.build_sorted(entries.into_iter().map(Ok)).unwrap();
        tree.validate().unwrap();
        assert!(tree.count_pages() > 8);
        tree.flush().unwrap();
        drop(tree);
        let tree = open(dir.path(), false);
        assert_eq!(
            tree.search(Mbr::new(-1.0, -1.0, 2000.0, 2000.0))
                .unwrap()
                .len(),
            12000
        );
        tree.validate().unwrap();
        drop(tree);
        let wrong = path.with_file_name("2.rtree");
        fs::copy(&path, &wrong).unwrap();
        assert!(SpatialIndexFile::quick_validate(wrong).is_err());
        fs::remove_file(&path).unwrap();
        assert!(SpatialIndexFile::quick_validate(path.clone()).is_err());
        assert!(!path.exists());
    }
}
