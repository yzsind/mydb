//! Streaming construction and traversal of immutable summary generations.
//! Only one page-sized pending node per level is retained while building.
//! Directory entries hold a subtree's lower bound and physical page number;
//! children are written before their parents. No resident address map exists.

use super::*;
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};

const CHILD_BYTES: usize = 8;
const FANOUT: usize = (PAGE_SIZE - HEADER) / (SLOT + CHILD_BYTES);
pub(super) const MAX_HEIGHT: usize = 12;
pub(super) const ROOT_KEY: Key = Key {
    column: 0,
    kind: Kind::Meta as u8,
    zone: 0,
};

#[derive(Clone, Copy, Debug)]
struct Child {
    first: Key,
    page: u64,
}

/// Root metadata stored in page zero. An unfinalized root is never usable for
/// pruning. This is a file-local construction barrier, not a substitute for
/// the database's snapshot/mutation-epoch publication check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::storage::core) struct Root {
    pub schema: u64,
    pub zone_pages: u32,
    pub page: u64,
    pub height: u32,
    pub next_page: u64,
    pub records: u64,
    pub revision: u64,
    pub missing_is_empty: bool,
    pub pending_records: u64,
}

/// A query-local directory hint, valid only for its immutable root snapshot.
/// It retains page addresses and key limits, never page guards or values.
#[derive(Clone, Copy)]
pub(super) struct LeafHint {
    page: u64,
    lower: Option<Key>,
    upper: Option<Key>,
}

impl LeafHint {
    pub(super) fn contains(self, key: Key) -> bool {
        self.lower.is_none_or(|lower| key >= lower) && self.upper.is_none_or(|upper| key < upper)
    }
}

impl Root {
    pub(super) fn payload(self, ready: bool) -> [u8; 64] {
        let mut out = [0u8; 64];
        write_u64(&mut out, 0, self.schema);
        write_u32(&mut out, 8, self.zone_pages);
        write_u32(&mut out, 12, self.height);
        write_u64(&mut out, 16, self.page);
        write_u64(&mut out, 24, self.next_page);
        write_u64(&mut out, 32, self.records);
        out[40] = u8::from(ready);
        out[41] = u8::from(self.missing_is_empty);
        write_u64(&mut out, 48, self.revision);
        write_u64(&mut out, 56, self.pending_records);
        out
    }

    /// Monotonic revocation of this immutable generation. The root remains
    /// unpublishable even after eviction/reopen; only a fresh generation can
    /// restore trust. A DML caller must not commit if this WAL write fails.
    pub fn invalidate(self, file: &PageFile) -> Result<u64, QueryError> {
        file.write(0, Kind::Meta, 0, &[(ROOT_KEY, &self.payload(false))])
    }

    pub fn open(file: &PageFile, schema: u64, zone_pages: u32) -> Result<Self, QueryError> {
        let (root, ready) = Self::load(file, schema, zone_pages)?;
        if !ready {
            return Err(corrupt("unpublished or invalid root"));
        }
        Ok(root)
    }

    pub fn load(file: &PageFile, schema: u64, zone_pages: u32) -> Result<(Self, bool), QueryError> {
        file.visit(0, AccessIntent::Normal, |view| {
            if view.kind() != Kind::Meta || view.len() != 1 || view.next() != 0 {
                return Err(corrupt("invalid root page"));
            }
            let (key, bytes) = view.entry(0).ok_or_else(|| corrupt("missing root"))?;
            if key != ROOT_KEY
                || bytes.len() != 64
                || bytes[40] > 1
                || bytes[41] > 1
                || bytes[42..48] != [0; 6]
            {
                return Err(corrupt("unpublished or invalid root"));
            }
            let root = Self {
                schema: read_u64(bytes, 0),
                zone_pages: read_u32(bytes, 8),
                height: read_u32(bytes, 12),
                page: read_u64(bytes, 16),
                next_page: read_u64(bytes, 24),
                records: read_u64(bytes, 32),
                revision: read_u64(bytes, 48),
                missing_is_empty: bytes[41] != 0,
                pending_records: read_u64(bytes, 56),
            };
            if root.schema != schema
                || root.zone_pages != zone_pages
                || zone_pages == 0
                || root.height as usize > MAX_HEIGHT
                || root.page == 0
                || root.page >= root.next_page
                || root.next_page.checked_mul(PAGE_SIZE as u64).is_none()
            {
                return Err(corrupt("invalid root identity or bounds"));
            }
            Ok((root, bytes[40] == 1))
        })
    }

    /// An exact point probe; the borrowed value cannot outlive the pinned
    /// leaf callback. Directory pins are released before loading a child.
    pub fn get<T>(
        self,
        file: &PageFile,
        key: Key,
        consume: impl FnOnce(Option<&[u8]>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        self.get_with_hint(file, key, &mut None, consume)
    }

    pub(super) fn get_with_hint<T>(
        self,
        file: &PageFile,
        key: Key,
        hint: &mut Option<LeafHint>,
        consume: impl FnOnce(Option<&[u8]>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        if let Some(hint) = hint.filter(|hint| hint.contains(key)) {
            return file.visit(hint.page, AccessIntent::Normal, |view| {
                validate_leaf(&view, hint.lower, hint.upper)?;
                let found = view
                    .entry(view.lower_bound(key))
                    .filter(|(candidate, _)| *candidate == key)
                    .map(|(_, bytes)| bytes);
                consume(found)
            });
        }
        let mut page = self.page;
        let mut upper = None;
        let mut lower = None;
        for _ in 0..self.height {
            let branch = file.visit(page, AccessIntent::Normal, |view| {
                validate_node(&view, Kind::Directory, lower, upper)?;
                let index = view.lower_bound(key);
                let index = if view
                    .entry(index)
                    .is_some_and(|(candidate, _)| candidate == key)
                {
                    index
                } else if index == 0 {
                    return Ok(None);
                } else {
                    index - 1
                };
                let (first, bytes) = view.entry(index).ok_or_else(|| corrupt("missing child"))?;
                let child = decode_child(bytes, page, self.next_page)?;
                let end = view.entry(index + 1).map(|(key, _)| key).or(upper);
                Ok(Some((child, first, end)))
            })?;
            match branch {
                Some((child, first, end)) => {
                    page = child;
                    lower = Some(first);
                    upper = end;
                }
                None => return consume(None),
            }
        }
        *hint = Some(LeafHint { page, lower, upper });
        file.visit(page, AccessIntent::Normal, |view| {
            validate_leaf(&view, lower, upper)?;
            let found = view
                .entry(view.lower_bound(key))
                .filter(|(candidate, _)| *candidate == key)
                .map(|(_, bytes)| bytes);
            consume(found)
        })
    }

    /// Visit [start, end). Directory traversal copies only the selected child
    /// descriptor, unpins the parent, then visits the child. Returning false
    /// cancels promptly and never retains a guard across user code on another
    /// page. At most one buffer frame is pinned at a time.
    pub fn visit_range(
        self,
        file: &PageFile,
        start: Key,
        end: Key,
        mut consume: impl FnMut(Key, &[u8]) -> Result<bool, QueryError>,
    ) -> Result<(), QueryError> {
        if start >= end {
            return Ok(());
        }
        self.visit_subtree(
            file,
            self.page,
            self.height,
            None,
            None,
            start,
            end,
            &mut consume,
        )?;
        Ok(())
    }

    fn visit_subtree(
        self,
        file: &PageFile,
        page: u64,
        height: u32,
        lower: Option<Key>,
        upper: Option<Key>,
        start: Key,
        end: Key,
        consume: &mut impl FnMut(Key, &[u8]) -> Result<bool, QueryError>,
    ) -> Result<bool, QueryError> {
        if height == 0 {
            return file.visit(page, AccessIntent::Normal, |view| {
                validate_leaf(&view, lower, upper)?;
                for index in view.lower_bound(start)..view.len() {
                    let (key, value) = view
                        .entry(index)
                        .ok_or_else(|| corrupt("missing leaf slot"))?;
                    if key >= end {
                        break;
                    }
                    if !consume(key, value)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            });
        }
        // This descriptor vector is bounded by one directory page and is
        // scratch-charged. It avoids re-pinning/checksumming the same parent
        // for every child, while not pinning it across recursive traversal.
        let _scratch = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::StorageScratch,
            FANOUT * std::mem::size_of::<(Child, Option<Key>)>(),
        )
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "zone map range scratch budget exceeded",
            )
        })?;
        let children = file.visit(page, AccessIntent::Normal, |view| {
            validate_node(&view, Kind::Directory, lower, upper)?;
            let mut children = Vec::with_capacity(view.len().min(FANOUT));
            for index in 0..view.len() {
                let (first, bytes) = view.entry(index).ok_or_else(|| corrupt("missing child"))?;
                let limit = view.entry(index + 1).map(|(key, _)| key).or(upper);
                if first >= end {
                    break;
                }
                if limit.is_some_and(|limit| limit <= start) {
                    continue;
                }
                children.push((
                    Child {
                        first,
                        page: decode_child(bytes, page, self.next_page)?,
                    },
                    limit,
                ));
            }
            Ok(children)
        })?;
        for (child, limit) in children {
            if !self.visit_subtree(
                file,
                child.page,
                height - 1,
                Some(child.first),
                limit,
                start,
                end,
                consume,
            )? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

pub(super) fn decode_child(bytes: &[u8], parent: u64, next_page: u64) -> Result<u64, QueryError> {
    if bytes.len() != CHILD_BYTES {
        return Err(corrupt("invalid child pointer"));
    }
    let page = read_u64(bytes, 0);
    // Mutable splits may append a child after its parent. Traversal is
    // bounded by the validated root height and verifies directory/leaf kind
    // and subtree intervals on every level; longer cycles cannot run forever.
    if page == 0 || page == parent || page >= next_page {
        return Err(corrupt("cyclic or out-of-bounds child pointer"));
    }
    Ok(page)
}

pub(super) fn validate_node(
    view: &View<'_>,
    kind: Kind,
    lower: Option<Key>,
    upper: Option<Key>,
) -> Result<(), QueryError> {
    if view.kind() != kind || view.next() != 0 || view.len() == 0 || view.len() > FANOUT {
        return Err(corrupt("invalid directory node"));
    }
    validate_range(view, lower, upper)
}

pub(super) fn validate_leaf(
    view: &View<'_>,
    lower: Option<Key>,
    upper: Option<Key>,
) -> Result<(), QueryError> {
    if !matches!(
        view.kind(),
        Kind::Bounds | Kind::Membership | Kind::ZoneMeta | Kind::Records
    ) || view.next() != 0
    {
        return Err(corrupt("invalid summary leaf"));
    }
    if view.len() == 0 && lower.is_some() {
        return Err(corrupt("empty referenced leaf"));
    }
    validate_range(view, lower, upper)
}

fn validate_range(
    view: &View<'_>,
    lower: Option<Key>,
    upper: Option<Key>,
) -> Result<(), QueryError> {
    if lower.is_some_and(|lo| view.entry(0).is_none_or(|(first, _)| lo != first))
        || upper.is_some_and(|hi| {
            view.len() > 0
                && view
                    .entry(view.len() - 1)
                    .is_some_and(|(last, _)| last >= hi)
        })
    {
        return Err(corrupt("subtree keys outside directory range"));
    }
    Ok(())
}

#[cfg(test)]
struct Level {
    children: Vec<Child>,
    _charge: ControlledMemoryCharge,
}

#[cfg(test)]
pub(super) struct Builder<'a> {
    file: &'a PageFile,
    root: Root,
    levels: Vec<Level>,
    last_key: Option<Key>,
    failed: bool,
}

#[cfg(test)]
impl<'a> Builder<'a> {
    pub fn new(file: &'a PageFile, schema: u64, zone_pages: u32) -> Result<Self, QueryError> {
        if zone_pages == 0 {
            return Err(corrupt("zero zone size"));
        }
        // A builder owns a fresh staging file. Rewriting a live file could
        // invalidate a reader that already captured its root. Filesystem
        // generation allocation/exclusion remains the caller's responsibility.
        if file.key.path.exists() || file.visit(0, AccessIntent::Normal, |_| Ok(())).is_ok() {
            return Err(corrupt("generation build requires a fresh file"));
        }
        let root = Root {
            schema,
            zone_pages,
            page: 0,
            height: 0,
            next_page: 1,
            records: 0,
            revision: 0,
            missing_is_empty: false,
            pending_records: 0,
        };
        file.write(0, Kind::Meta, 0, &[(ROOT_KEY, &root.payload(false))])?;
        Ok(Self {
            file,
            root,
            levels: Vec::new(),
            last_key: None,
            failed: false,
        })
    }

    /// Input is already page-sized and sorted. The producer can aggregate
    /// typed summaries incrementally without retaining the whole generation.
    pub fn push_leaf(&mut self, kind: Kind, entries: &[(Key, &[u8])]) -> Result<(), QueryError> {
        if self.failed {
            return Err(corrupt("generation builder previously failed"));
        }
        // Any failure poisons publication, including a failure after a leaf
        // was logged but before its parent descriptor could be admitted.
        self.failed = true;
        if !matches!(kind, Kind::Bounds | Kind::Membership | Kind::ZoneMeta) || entries.is_empty() {
            return Err(corrupt("invalid build leaf"));
        }
        if self.last_key.is_some_and(|last| last >= entries[0].0) {
            return Err(corrupt("build keys are not strictly ordered"));
        }
        let page = self.root.next_page;
        self.file.write(page, kind, 0, entries)?;
        self.root.next_page = page
            .checked_add(1)
            .ok_or_else(|| corrupt("page allocation overflow"))?;
        self.root.records = self
            .root
            .records
            .checked_add(entries.len() as u64)
            .ok_or_else(|| corrupt("record count overflow"))?;
        self.last_key = entries.last().map(|(key, _)| *key);
        self.push_child(
            0,
            Child {
                first: entries[0].0,
                page,
            },
        )?;
        self.failed = false;
        Ok(())
    }

    fn push_child(&mut self, level: usize, child: Child) -> Result<(), QueryError> {
        if level > MAX_HEIGHT {
            return Err(corrupt("directory height limit"));
        }
        while self.levels.len() <= level {
            let charge = ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::StorageScratch,
                FANOUT * std::mem::size_of::<Child>(),
            )?;
            self.levels.push(Level {
                children: Vec::with_capacity(FANOUT),
                _charge: charge,
            });
        }
        if self.levels[level].children.len() == FANOUT {
            let parent = self.flush_level(level)?;
            self.push_child(level + 1, parent)?;
        }
        self.levels[level].children.push(child);
        Ok(())
    }

    fn flush_level(&mut self, level: usize) -> Result<Child, QueryError> {
        let children = &self.levels[level].children;
        let first = children
            .first()
            .ok_or_else(|| corrupt("empty directory build level"))?
            .first;
        let _scratch = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::StorageScratch,
            FANOUT * (8 + std::mem::size_of::<(Key, &[u8])>()),
        )?;
        let pointers: Vec<_> = children
            .iter()
            .map(|child| child.page.to_le_bytes())
            .collect();
        let entries: Vec<_> = children
            .iter()
            .zip(&pointers)
            .map(|(child, bytes)| (child.first, bytes.as_slice()))
            .collect();
        let page = self.root.next_page;
        self.file.write(page, Kind::Directory, 0, &entries)?;
        self.root.next_page = page
            .checked_add(1)
            .ok_or_else(|| corrupt("page allocation overflow"))?;
        self.levels[level].children.clear();
        Ok(Child { first, page })
    }

    pub fn finish(mut self) -> Result<Root, QueryError> {
        if self.failed {
            return Err(corrupt("cannot publish a failed generation build"));
        }
        if self.levels.is_empty() {
            self.file.write(1, Kind::Bounds, 0, &[])?;
            self.root.page = 1;
            self.root.next_page = 2;
        } else {
            for level in 0..=MAX_HEIGHT {
                let higher = self
                    .levels
                    .iter()
                    .skip(level + 1)
                    .any(|level| !level.children.is_empty());
                if !higher && self.levels[level].children.len() == 1 {
                    self.root.page = self.levels[level].children[0].page;
                    self.root.height = level as u32;
                    break;
                }
                let parent = self.flush_level(level)?;
                self.push_child(level + 1, parent)?;
            }
        }
        if self.root.page == 0 {
            return Err(corrupt("directory root not constructed"));
        }
        self.file
            .write(0, Kind::Meta, 0, &[(ROOT_KEY, &self.root.payload(true))])?;
        Ok(self.root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(dir: &Path) -> PageFile {
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir).unwrap()));
        let pool = BufferPool::new(
            8,
            Arc::clone(&log),
            DurabilityMode::Relaxed,
            Duration::from_secs(5),
        );
        let path = crate::storage::naming::table_object_path(dir, "test", "paged", 17, "zm");
        PageFile::new(path, 17, 1, pool, log, DurabilityMode::Relaxed).unwrap()
    }

    fn key(column: u32, zone: u64) -> Key {
        Key {
            column,
            kind: Kind::Bounds as u8,
            zone,
        }
    }

    #[test]
    fn zone_map_page_directory_streams_multilevel_and_selects_columns() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let file = file(dir.path());
        let mut builder = Builder::new(&file, 77, 64).unwrap();
        assert!(Root::open(&file, 77, 64).is_err());
        for column in 1..=3 {
            for zone in 0..250 {
                builder
                    .push_leaf(Kind::Bounds, &[(key(column, zone), &zone.to_le_bytes())])
                    .unwrap();
            }
        }
        let root = builder.finish().unwrap();
        assert_eq!(root.height, 2);
        assert_eq!(root.records, 750);
        file.flush().unwrap();
        file.pool.flush_and_evict_all().unwrap();
        assert_eq!(Root::open(&file, 77, 64).unwrap(), root);
        assert!(Root::open(&file, 78, 64).is_err());
        for (column, zone) in [(1, 0), (2, 32), (3, 249)] {
            root.get(&file, key(column, zone), |bytes| {
                assert_eq!(bytes, Some(zone.to_le_bytes().as_slice()));
                Ok(())
            })
            .unwrap();
        }
        root.get(&file, key(2, 251), |bytes| {
            assert!(bytes.is_none());
            Ok(())
        })
        .unwrap();
        let before = file.pool.snapshot().read_ios;
        let mut found = Vec::new();
        root.visit_range(&file, key(2, 100), key(2, 110), |k, bytes| {
            assert_eq!(read_u64(bytes, 0), k.zone);
            found.push(k.zone);
            Ok(true)
        })
        .unwrap();
        assert_eq!(found, (100..110).collect::<Vec<_>>());
        assert!(
            file.pool.snapshot().read_ios - before <= 14,
            "must not load unrelated leaves"
        );
        let mut calls = 0;
        root.visit_range(&file, key(1, 0), key(4, 0), |_, _| {
            calls += 1;
            Ok(false)
        })
        .unwrap();
        assert_eq!(calls, 1);
    }

    #[test]
    fn zone_map_page_directory_empty_and_abandoned_builds() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let file = file(dir.path());
        let root = Builder::new(&file, 1, 64).unwrap().finish().unwrap();
        assert_eq!(root.records, 0);
        root.get(&file, key(1, 0), |value| {
            assert!(value.is_none());
            Ok(())
        })
        .unwrap();
        assert!(Builder::new(&file, 1, 64).is_err());
        let abandoned = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let file = super::tests::file(abandoned.path());
        let mut builder = Builder::new(&file, 1, 64).unwrap();
        builder
            .push_leaf(Kind::Bounds, &[(key(1, 0), b"a")])
            .unwrap();
        assert!(builder
            .push_leaf(Kind::Bounds, &[(key(1, 0), b"b")])
            .is_err());
        assert!(builder.finish().is_err());
        file.flush().unwrap();
        file.pool.flush_and_evict_all().unwrap();
        assert!(Root::open(&file, 1, 64).is_err());
    }

    #[test]
    fn zone_map_page_directory_rejects_cycles_and_wrong_subtree_ranges() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let file = file(dir.path());
        let mut builder = Builder::new(&file, 1, 64).unwrap();
        builder
            .push_leaf(Kind::Bounds, &[(key(1, 0), b"a")])
            .unwrap();
        builder
            .push_leaf(Kind::Bounds, &[(key(2, 0), b"b")])
            .unwrap();
        let root = builder.finish().unwrap();
        file.write(
            root.page,
            Kind::Directory,
            0,
            &[(key(1, 0), &root.page.to_le_bytes())],
        )
        .unwrap();
        assert!(root.get(&file, key(1, 0), |_| Ok(())).is_err());
        file.write(
            root.page,
            Kind::Directory,
            0,
            &[(key(1, 0), &2u64.to_le_bytes())],
        )
        .unwrap();
        assert!(root.get(&file, key(1, 0), |_| Ok(())).is_err());
    }

    #[test]
    fn zone_map_page_directory_process_crash_recovers_only_published_roots() {
        const CHILD_DIR: &str = "MYDB_ZONE_MAP_PAGE_CRASH_DIR";
        const CHILD_STAGE: &str = "MYDB_ZONE_MAP_PAGE_CRASH_STAGE";
        if let Some(path) = std::env::var_os(CHILD_DIR) {
            let stage: u32 = std::env::var(CHILD_STAGE).unwrap().parse().unwrap();
            let file = file(Path::new(&path));
            let mut builder = Builder::new(&file, 77, 64).unwrap();
            if stage >= 1 {
                builder
                    .push_leaf(Kind::Bounds, &[(key(1, 0), b"first")])
                    .unwrap();
                builder
                    .push_leaf(Kind::Bounds, &[(key(2, 0), b"second")])
                    .unwrap();
            }
            if stage >= 2 {
                let root = builder.finish().unwrap();
                if stage == 3 {
                    root.invalidate(&file).unwrap();
                }
            }
            let lsn = file.log.lock().last_lsn();
            LogManager::sync_durable_to(&file.log, lsn).unwrap();
            // No Rust destructors, database shutdown, or buffer flush. Only
            // WAL durability is guaranteed at the simulated crash boundary.
            std::process::exit(73);
        }
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        for stage in 0..=3 {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let result = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "storage::core::zone_map_page::directory::tests::zone_map_page_directory_process_crash_recovers_only_published_roots", "--nocapture"])
                .env(CHILD_DIR, dir.path())
                .env(CHILD_STAGE, stage.to_string())
                .output().unwrap();
            assert_eq!(
                result.status.code(),
                Some(73),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            let recovery = recover_from_wal(dir.path(), DurabilityMode::Strict).unwrap();
            assert!(recovery.summary.redone_records > 0);
            let file = file(dir.path());
            let root = Root::open(&file, 77, 64);
            if stage != 2 {
                assert!(
                    root.is_err(),
                    "an incomplete or revoked generation must not prune"
                );
            } else {
                let root = root.unwrap();
                assert_eq!(root.records, 2);
                root.get(&file, key(1, 0), |value| {
                    assert_eq!(value, Some(b"first".as_slice()));
                    Ok(())
                })
                .unwrap();
                root.get(&file, key(2, 0), |value| {
                    assert_eq!(value, Some(b"second".as_slice()));
                    Ok(())
                })
                .unwrap();
            }
        }
    }
}
