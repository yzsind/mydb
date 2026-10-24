//! Immutable local repository objects and append-only, hash-linked publication.
use super::*;
use aes_gcm::aead::Payload;
#[path = "catalog_checkpoint.rs"]
mod checkpoint;
#[path = "catalog_history.rs"]
mod history;

pub(crate) const FORMAT: &str = crate::storage::format_versions::BACKUP_REPOSITORY_FORMAT;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Header {
    pub format: String,
    pub id: String,
    pub storage_fingerprint: String,
    pub wal_key_id: Option<String>,
    pub key_fingerprint: Option<String>,
    pub wal_encoding: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Position {
    pub generation: u64,
    pub digest: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Base {
    pub id: String,
    pub repository_id: String,
    pub source_id: String,
    pub timeline_id: String,
    pub stream_id: Option<String>,
    pub source_end_lsn: u64,
    pub source_end_time: u64,
    pub marker_digest: String,
    pub seal_digest: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceSpan {
    pub segment_id: u64,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub captured_valid_len: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Range {
    pub id: String,
    pub stream_id: String,
    pub source_id: String,
    pub timeline_id: String,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub covered_time: u64,
    pub frame_count: u64,
    pub raw_bytes: u64,
    pub source_spans: Vec<SourceSpan>,
    pub chunks: Vec<ChunkRef>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamDescriptor {
    pub version: u32,
    pub repository_id: String,
    pub stream_id: String,
    pub source_id: String,
    pub timeline_id: String,
    pub anchor_base: String,
    pub anchor_lsn: u64,
    pub anchor_time: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TimeFence {
    pub version: u32,
    pub stream_id: String,
    pub lsn: u64,
    pub time: u64,
    pub anchor: Position,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedFence {
    fence: TimeFence,
    digest: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub(crate) enum Publication {
    Checkpoint {
        anchor: Position,
        digest: String,
        records: u64,
        history: String,
    },
    Base {
        base: Base,
    },
    Range {
        range: Range,
    },
    Fence {
        stream_id: String,
        lsn: u64,
        time: u64,
    },
    Close {
        stream_id: String,
        lsn: u64,
        time: u64,
        discarded: bool,
    },
    Prune {
        bases: Vec<String>,
        ranges: Vec<String>,
    },
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Event {
    #[serde(skip)]
    _memory: Option<Arc<ControlledMemoryCharge>>,
    generation: u64,
    previous: String,
    publication: Publication,
}

pub(crate) struct Lease(std::fs::File);
impl Lease {
    pub(crate) fn acquire(root: &Path, exclusive: bool) -> Result<Self, QueryError> {
        let file = std::fs::File::open(root.join("gc.lock"))
            .map_err(|e| io_err("open repository GC lease", e))?;
        if exclusive {
            file.try_lock()
        } else {
            file.try_lock_shared()
        }
        .map_err(|e| backup_err(format!("repository is busy: {e}")))?;
        Ok(Self(file))
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}
struct PublishLease(std::fs::File);
impl Drop for PublishLease {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

pub(crate) struct Repository {
    pub root: PathBuf,
    pub header: Header,
    pub key: Option<CryptoKey>,
    pub(super) history_index_cache: Mutex<Option<(String, Arc<history::HistoryIndex>)>>,
    pub(super) history_pack_cache: Mutex<Option<(String, Arc<history::PackBuffer>)>>,
}
fn json<T: Serialize>(value: &T) -> Result<Vec<u8>, QueryError> {
    serde_json::to_vec(value).map_err(|e| backup_err(e.to_string()))
}
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, QueryError> {
    serde_json::from_slice(&read_bounded(path, MAX_METADATA_BYTES)?)
        .map_err(|e| backup_err(format!("invalid {}: {e}", path.display())))
}
impl Repository {
    pub(crate) fn init(
        root: &Path,
        key_id: Option<&str>,
        key_file: Option<&Path>,
    ) -> Result<Self, QueryError> {
        Self::init_with_encoding(root, key_id, key_file, "ADAPTIVE_LZ4")
    }
    pub(crate) fn init_with_encoding(
        root: &Path,
        key_id: Option<&str>,
        key_file: Option<&Path>,
        encoding: &str,
    ) -> Result<Self, QueryError> {
        if key_id.is_none() && key_file.is_some() {
            return Err(backup_err(
                "--key-file requires --wal-key-id when initializing a repository",
            ));
        }
        if !["NONE", "ADAPTIVE_LZ4"].contains(&encoding) {
            return Err(backup_err("WAL encoding must be NONE or ADAPTIVE_LZ4"));
        }
        let resolved = resolved_destination(root)?;
        let root = resolved.as_path();
        if root.join("repository.json").exists() {
            DirectoryOperation::cleanup(root, "repository-init")?;
            DirectoryOperation::cleanup(root, "import-work")?;
            let repo = Self::open(root, key_file)?;
            if repo.header.wal_key_id.as_deref() != key_id || repo.header.wal_encoding != encoding {
                return Err(backup_err("repository WAL key configuration is immutable"));
            }
            sync_dir(
                root.parent()
                    .ok_or_else(|| backup_err("repository has no parent"))?,
            )?;
            return Ok(repo);
        }
        if root.exists()
            && fs::read_dir(root)
                .map_err(|e| io_err("inspect repository destination", e))?
                .next()
                .is_some()
        {
            return Err(backup_err(
                "repository is nonempty or uses an unsupported legacy format",
            ));
        }
        let key = resolve_key(key_id, key_file)?;
        let operation = DirectoryOperation::new(root, "repository-init")?;
        let destination = root;
        let root = operation.data.as_path();
        for name in [
            "bases",
            "streams",
            "blobs",
            "staging",
            "catalog/generations",
            "catalog/checkpoints",
            "catalog/history/packs",
            "catalog/history/indexes",
        ] {
            fs::create_dir_all(root.join(name))
                .map_err(|e| io_err("create private repository namespace", e))?;
        }
        fs::create_dir_all(root.join("blobs").join(key_id.unwrap_or("plain")))
            .map_err(|e| io_err("create private WAL namespace", e))?;
        for name in ["gc.lock", "publish.lock"] {
            atomic_write_new(&root.join(name), b"")?;
        }
        #[cfg(test)]
        super::faults::hit("repository_init_ready", destination)?;
        let header = Header {
            format: FORMAT.into(),
            id: random_uuid(),
            storage_fingerprint: crate::storage::format_versions::native_replication_compatibility(
            ),
            wal_key_id: key_id.map(str::to_owned),
            key_fingerprint: key.as_ref().map(|k| sha256_hex(&k.bytes)),
            wal_encoding: encoding.into(),
        };
        atomic_write_new(&root.join("catalog/CURRENT"), &json(&Position::default())?)?;
        atomic_write_new(&root.join("repository.json"), &json(&header)?)?;
        operation.install()?;
        Ok(Self {
            root: destination.to_path_buf(),
            header,
            key,
            history_index_cache: Mutex::new(None),
            history_pack_cache: Mutex::new(None),
        })
    }
    pub(crate) fn open(root: &Path, key_file: Option<&Path>) -> Result<Self, QueryError> {
        let resolved = resolved_destination(root)?;
        let root = resolved.as_path();
        let header: Header = read_json(&root.join("repository.json"))?;
        if header.format != FORMAT
            || header.storage_fingerprint
                != crate::storage::format_versions::native_replication_compatibility()
        {
            return Err(backup_err(
                "unsupported repository/storage format; legacy repositories are not upgraded",
            ));
        }
        validate_id(&header.id)?;
        if header.wal_key_id.is_some() != header.key_fingerprint.is_some() {
            return Err(backup_err("inconsistent WAL key configuration"));
        }
        if let Some(id) = &header.wal_key_id {
            validate_id(id)?;
            if id == "plain" {
                return Err(backup_err("reserved WAL key namespace"));
            }
        }
        if let Some(hash) = &header.key_fingerprint {
            validate_hash(hash)?;
        }
        if header.wal_encoding != "ADAPTIVE_LZ4" && header.wal_encoding != "NONE" {
            return Err(backup_err("unknown WAL encoding"));
        }
        for name in [
            "gc.lock",
            "publish.lock",
            "bases",
            "streams",
            "blobs",
            "staging",
            "catalog",
            "catalog/generations",
            "catalog/checkpoints",
            "catalog/history/packs",
            "catalog/history/indexes",
            "catalog/CURRENT",
        ] {
            reject_symlink_path(root, Path::new(name))?;
            fs::metadata(root.join(name)).map_err(|e| io_err("open repository entry", e))?;
        }
        let namespace = root
            .join("blobs")
            .join(header.wal_key_id.as_deref().unwrap_or("plain"));
        reject_symlink_path(root, namespace.strip_prefix(root).unwrap())?;
        if !namespace.is_dir() {
            return Err(backup_err("durable repository WAL namespace is missing"));
        }
        let key = if header.wal_key_id.is_some() && key_file.is_some() {
            resolve_key(header.wal_key_id.as_deref(), key_file)?
        } else {
            None
        };
        let repo = Self {
            root: root.to_path_buf(),
            header,
            key,
            history_index_cache: Mutex::new(None),
            history_pack_cache: Mutex::new(None),
        };
        if let Some(key) = &repo.key {
            repo.check_key(key)?;
        }
        Ok(repo)
    }
    fn check_key(&self, key: &CryptoKey) -> Result<(), QueryError> {
        if self.header.wal_key_id.as_deref() != Some(&key.id)
            || self.header.key_fingerprint.as_deref() != Some(&sha256_hex(&key.bytes))
        {
            return Err(backup_err("WAL key material/namespace mismatch"));
        }
        Ok(())
    }
    pub(crate) fn require_key(&mut self, key_file: Option<&Path>) -> Result<(), QueryError> {
        if self.header.wal_key_id.is_some() && self.key.is_none() {
            let key = resolve_key(self.header.wal_key_id.as_deref(), key_file)?
                .ok_or_else(|| backup_err("missing WAL key"))?;
            self.check_key(&key)?;
            self.key = Some(key);
        }
        Ok(())
    }
    pub(crate) fn register_stream(&self, descriptor: &StreamDescriptor) -> Result<(), QueryError> {
        validate_id(&descriptor.stream_id)?;
        validate_id(&descriptor.anchor_base)?;
        if descriptor.version != 1 || descriptor.repository_id != self.header.id {
            return Err(backup_err("stream repository identity mismatch"));
        }
        let directory = self.root.join("streams").join(&descriptor.stream_id);
        reject_symlink_path(&self.root, directory.strip_prefix(&self.root).unwrap())?;
        create_directory_durable(&directory)?;
        let path = directory.join("descriptor");
        let bytes = json(descriptor)?;
        if path.exists() {
            if read_bounded(&path, MAX_METADATA_BYTES)? != bytes {
                return Err(backup_err("conflicting immutable stream descriptor"));
            }
        } else {
            atomic_write_new(&path, &bytes)?;
        }
        create_directory_durable(&directory.join("ranges"))?;
        create_directory_durable(&directory.join("fences"))?;
        // Stream setup persists its reusable private publication slot once.
        drop(DirectoryOperation::new(
            &self.root.join("staging/range-work"),
            "range-work",
        )?);
        Ok(())
    }
    pub(crate) fn stream(&self, id: &str) -> Result<StreamDescriptor, QueryError> {
        validate_id(id)?;
        let relative = PathBuf::from("streams").join(id).join("descriptor");
        reject_symlink_path(&self.root, &relative)?;
        let value: StreamDescriptor = read_json(&self.root.join(relative))?;
        if value.version != 1 || value.stream_id != id || value.repository_id != self.header.id {
            return Err(backup_err("invalid immutable stream descriptor"));
        }
        Ok(value)
    }
    /// One replaceable, checksummed fence per stream. Readers copy its value.
    pub(crate) fn publish_fence(
        &self,
        id: &str,
        lsn: u64,
        time: u64,
    ) -> Result<Position, QueryError> {
        let _lock = self.publish_lock()?;
        self.stream(id)?;
        let anchor = self.position()?;
        if let Some(old) = self.read_fence(id)? {
            if old.lsn > lsn || old.time > time {
                return Err(backup_err("time fence regressed"));
            }
        }
        let fence = TimeFence {
            version: 1,
            stream_id: id.into(),
            lsn,
            time,
            anchor: anchor.clone(),
        };
        let digest = sha256_hex(&json(&fence)?);
        let path = self.root.join("streams").join(id).join("fences/current");
        atomic_replace(&path, &json(&CheckedFence { fence, digest })?)?;
        Ok(anchor)
    }
    fn read_fence(&self, id: &str) -> Result<Option<TimeFence>, QueryError> {
        validate_id(id)?;
        let relative = PathBuf::from("streams").join(id).join("fences/current");
        reject_symlink_path(&self.root, &relative)?;
        let path = self.root.join(relative);
        if !path.exists() {
            return Ok(None);
        }
        let value: CheckedFence = read_json(&path)?;
        if value.fence.version != 1
            || value.fence.time == u64::MAX
            || value.fence.lsn == u64::MAX
            || value.fence.stream_id != id
            || sha256_hex(&json(&value.fence)?) != value.digest
        {
            return Err(backup_err("invalid time fence proof"));
        }
        Ok(Some(value.fence))
    }
    pub(crate) fn fence_at(
        &self,
        id: &str,
        position: &Position,
    ) -> Result<Option<TimeFence>, QueryError> {
        let _lock = self.publish_lock()?;
        self.stream(id)?;
        let Some(fence) = self.read_fence(id)? else {
            return Ok(None);
        };
        if fence.anchor.generation > position.generation {
            return Ok(None);
        }
        self.visit_since(position, &fence.anchor, |_| Ok(()))?;
        Ok(Some(fence))
    }
    pub(crate) fn position(&self) -> Result<Position, QueryError> {
        read_json(&self.root.join("catalog/CURRENT"))
    }
    fn publish_lock(&self) -> Result<PublishLease, QueryError> {
        let file = std::fs::File::open(self.root.join("publish.lock"))
            .map_err(|e| io_err("open catalog publication lock", e))?;
        file.lock()
            .map_err(|e| io_err("lock catalog publication", e))?;
        Ok(PublishLease(file))
    }
    pub(crate) fn publish(&self, publication: Publication) -> Result<Position, QueryError> {
        // Also confirm an installation left visible by an earlier parent-sync error.
        sync_dir(
            self.root
                .parent()
                .ok_or_else(|| backup_err("repository has no parent"))?,
        )?;
        self.append_publication(publication, false)
    }
    fn append_publication(
        &self,
        publication: Publication,
        confirmed_root: bool,
    ) -> Result<Position, QueryError> {
        let _lock = self.publish_lock()?;
        let old = self.position()?;
        if confirmed_root && old.generation == 0 {
            return Err(backup_err(
                "WAL range requires a previously published baseline",
            ));
        }
        let position = self.append_locked(&old, publication)?;
        if position.generation % 1024 == 0 {
            return Ok(self.checkpoint_locked(&position)?.commit);
        }
        Ok(position)
    }
    fn append_locked(
        &self,
        old: &Position,
        publication: Publication,
    ) -> Result<Position, QueryError> {
        let event = Event {
            _memory: None,
            generation: old
                .generation
                .checked_add(1)
                .ok_or_else(|| backup_err("catalog generation overflow"))?,
            previous: old.digest.clone(),
            publication,
        };
        let bytes = json(&event)?;
        if bytes.len() > MAX_METADATA_BYTES {
            return Err(backup_err("catalog event exceeds metadata budget"));
        }
        let _disk = reserve_disk_write(
            &self.root,
            (bytes.len() as u64).saturating_mul(2).saturating_add(4096),
        )?;
        let position = Position {
            generation: event.generation,
            digest: sha256_hex(&bytes),
        };
        let path = self.root.join(format!(
            "catalog/generations/{:020}-{}",
            position.generation, position.digest
        ));
        atomic_write_new_ready(&path, &bytes, || {
            if read_bounded(&path, bytes.len())? != bytes {
                return Err(backup_err("conflicting immutable catalog event"));
            }
            Ok(())
        })?;
        atomic_replace(&self.root.join("catalog/CURRENT"), &json(&position)?)?;
        Ok(position)
    }
    pub(crate) fn confirm_position(&self, position: &Position) -> Result<(), QueryError> {
        if position.generation > 0 {
            let path = self.root.join(format!(
                "catalog/generations/{:020}-{}",
                position.generation, position.digest
            ));
            crate::platform::fs::sync_path(&path)
                .map_err(|e| io_err("confirm catalog event", e))?;
        }
        crate::platform::fs::sync_path(&self.root.join("catalog/CURRENT"))
            .map_err(|e| io_err("confirm catalog pointer", e))?;
        sync_dir(&self.root.join("catalog/generations"))?;
        sync_dir(&self.root.join("catalog"))
    }
    pub(crate) fn verify_coverage(&self, position: &Position) -> Result<(), QueryError> {
        let mut streams: BTreeMap<String, (u64, u64, u64)> = BTreeMap::new();
        self.visit_live(position, |_, event| {
            if let Publication::Base { base } = event {
                if let Some(stream) = base.stream_id {
                    let descriptor = self.stream(&stream)?;
                    if descriptor.source_id != base.source_id
                        || descriptor.timeline_id != base.timeline_id
                    {
                        return Err(backup_err("base source disagrees with stream descriptor"));
                    }
                    let state = streams.entry(stream).or_insert((
                        base.source_end_lsn,
                        base.source_end_lsn,
                        0,
                    ));
                    state.0 = state.0.min(base.source_end_lsn);
                    state.1 = state.1.max(base.source_end_lsn);
                }
            }
            Ok(())
        })?;
        self.visit_live(position, |_, event| {
            if let Publication::Range { range } = event {
                let state = streams
                    .get_mut(&range.stream_id)
                    .ok_or_else(|| backup_err("WAL stream has no retained baseline"))?;
                let descriptor = self.stream(&range.stream_id)?;
                if range.source_id != descriptor.source_id
                    || range.timeline_id != descriptor.timeline_id
                {
                    return Err(backup_err("range source disagrees with stream descriptor"));
                }
                if range.last_lsn <= state.0 {
                    return Ok(());
                }
                if state.2 != 0 && range.last_lsn + 1 != state.2 {
                    return Err(backup_err("committed WAL coverage has a gap or overlap"));
                }
                state.1 = state.1.max(range.last_lsn);
                state.2 = range.first_lsn;
            }
            Ok(())
        })?;
        for (stream, (floor, head, first)) in streams {
            if first > floor + 1 {
                return Err(backup_err("retained base cannot reach archived WAL"));
            }
            if let Some(fence) = self.fence_at(&stream, position)? {
                if fence.lsn > head {
                    return Err(backup_err("time fence claims missing WAL"));
                }
            }
        }
        Ok(())
    }
    /// Events are streamed backward from a fixed root; no complete history is retained.
    pub(crate) fn visit(
        &self,
        position: &Position,
        mut visit: impl FnMut(u64, Publication) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let mut current = position.clone();
        while current.generation > 0 {
            let event = self.checked_event(&current)?;
            visit(event.generation, event.publication)?;
            current = Position {
                generation: current.generation - 1,
                digest: event.previous,
            };
        }
        if !current.digest.is_empty() {
            return Err(backup_err("invalid catalog genesis"));
        }
        Ok(())
    }
    pub(crate) fn visit_live(
        &self,
        position: &Position,
        mut visit: impl FnMut(u64, Publication) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let mut bases = BTreeSet::new();
        let mut ranges = BTreeSet::new();
        let mut budget =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)?;
        let mut filter = |generation, event: Publication| {
            match &event {
                Publication::Prune {
                    bases: removed,
                    ranges: removed_ranges,
                } => {
                    for id in removed {
                        if bases.insert(id.clone()) {
                            budget.try_grow(160)?;
                        }
                    }
                    for id in removed_ranges {
                        if ranges.insert(id.clone()) {
                            budget.try_grow(160)?;
                        }
                    }
                    return Ok(());
                }
                Publication::Base { base } => {
                    if !bases.insert(base.id.clone()) {
                        return Ok(());
                    }
                    budget.try_grow(160)?;
                }
                Publication::Range { range } => {
                    if !ranges.insert(range.id.clone()) {
                        return Ok(());
                    }
                    budget.try_grow(160)?;
                }
                _ => {}
            }
            visit(generation, event)
        };
        if !self.visit_checkpoint(position, &mut filter)? {
            self.visit(position, &mut filter)?;
        }
        Ok(())
    }

    pub(crate) fn visit_since(
        &self,
        position: &Position,
        after: &Position,
        mut visit: impl FnMut(Publication) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let mut current = position.clone();
        if current.generation < after.generation {
            return Err(backup_err("catalog rollback"));
        }
        if current.generation.saturating_sub(after.generation) > 1024
            && self.visit_since_checkpoint(position, after, &mut visit)?
        {
            return Ok(());
        }
        while current.generation > after.generation {
            let event = self.checked_event(&current)?;
            visit(event.publication)?;
            current = Position {
                generation: current.generation - 1,
                digest: event.previous,
            };
        }
        if current != *after {
            return Err(backup_err("acknowledged catalog identity changed"));
        }
        Ok(())
    }

    pub(crate) fn base(&self, id: &str) -> Result<(Base, native::Snapshot), QueryError> {
        self.base_impl(id, true)
    }
    pub(crate) fn base_for_open(&self, id: &str) -> Result<(Base, native::Snapshot), QueryError> {
        self.base_impl(id, false)
    }
    fn base_impl(&self, id: &str, full_hash: bool) -> Result<(Base, native::Snapshot), QueryError> {
        validate_id(id)?;
        let path = self.root.join("bases").join(id);
        reject_symlink_path(&self.root, Path::new("bases").join(id).as_path())?;
        let base: Base = read_json(&path.join("manifest"))?;
        let seal = read_bounded(&path.join("seal"), MAX_METADATA_BYTES)?;
        let marker = read_bounded(&path.join("data").join(native::MARKER), MAX_METADATA_BYTES)?;
        let commit: String = read_json(&path.join("commit"))?;
        let manifest = json(&base)?;
        if base.id != id
            || base.repository_id != self.header.id
            || sha256_hex(&seal) != base.seal_digest
            || sha256_hex(&marker) != base.marker_digest
            || sha256_hex(&[manifest.as_slice(), seal.as_slice()].concat()) != commit
        {
            return Err(backup_err("base commit/manifest/seal mismatch"));
        }
        let image = if full_hash {
            native::verify(&path.join("data"))?
        } else {
            native::validate_open(&path.join("data"))?;
            native::inspect(&path.join("data"))?.ok_or_else(|| backup_err("missing image"))?
        };
        if image.kind != native::ImageKind::SealedBase
            || image.manifest.end_lsn != base.source_end_lsn
            || image.manifest.source_id != base.source_id
            || image.manifest.end_commit_unix_us != base.source_end_time
            || json(&image.seal)? != seal
        {
            return Err(backup_err("base image identity mismatch"));
        }
        Ok((base, image))
    }
    pub(crate) fn publish_base(
        &self,
        operation: &Path,
        base: Base,
        image: &native::Snapshot,
    ) -> Result<Position, QueryError> {
        let manifest = json(&base)?;
        let seal = json(&image.seal)?;
        write_sync(&operation.join("manifest"), &manifest)?;
        write_sync(&operation.join("seal"), &seal)?;
        write_sync(
            &operation.join("commit"),
            &json(&sha256_hex(
                &[manifest.as_slice(), seal.as_slice()].concat(),
            ))?,
        )?;
        sync_tree(operation)?;
        native::rename_new_directory(operation, &self.root.join("bases").join(&base.id))?;
        sync_dir(&self.root.join("bases"))?;
        #[cfg(test)]
        if FAIL_AFTER_BASE_RENAME.swap(false, Ordering::SeqCst) {
            return Err(backup_err("injected base publication interruption"));
        }
        self.publish(Publication::Base { base })
    }
    fn blob_path(&self, digest: &str) -> PathBuf {
        self.root
            .join("blobs")
            .join(self.header.wal_key_id.as_deref().unwrap_or("plain"))
            .join(digest)
    }
    pub(crate) fn encoded_blob_path(&self, reference: &ChunkRef) -> Result<PathBuf, QueryError> {
        validate_hash(&reference.hash)?;
        if reference.len == 0 || reference.len > CHUNK_BYTES as u64 {
            return Err(backup_err("invalid WAL blob reference"));
        }
        let path = self.blob_path(&reference.hash);
        reject_symlink_path(&self.root, path.strip_prefix(&self.root).unwrap())?;
        Ok(path)
    }
    pub(crate) fn store_blob(&self, bytes: &[u8]) -> Result<ChunkRef, QueryError> {
        super::resources::throttle(bytes.len() as u64)?;
        if bytes.is_empty() || bytes.len() > CHUNK_BYTES {
            return Err(backup_err("invalid WAL chunk size"));
        }
        let digest = sha256_hex(bytes);
        let path = self.blob_path(&digest);
        let aad = format!(
            "{}:{}:{}:{}:{}",
            FORMAT,
            self.header.id,
            self.header.wal_encoding,
            bytes.len(),
            digest
        );
        let encoded = encode_blob_with_encoding(
            bytes,
            &sha256_digest(bytes),
            None,
            self.header.wal_encoding == "ADAPTIVE_LZ4",
        )?;
        let stored = if let Some(key) = &self.key {
            self.check_key(key)?;
            let mut nonce = [0; 12];
            rand::rngs::OsRng.fill_bytes(&mut nonce);
            let cipher =
                Aes256Gcm::new_from_slice(&key.bytes).map_err(|_| backup_err("invalid WAL key"))?;
            let mut data = nonce.to_vec();
            data.extend(
                cipher
                    .encrypt(
                        Nonce::from_slice(&nonce),
                        Payload {
                            msg: &encoded,
                            aad: aad.as_bytes(),
                        },
                    )
                    .map_err(|_| backup_err("WAL encryption failed"))?,
            );
            data
        } else {
            if self.header.wal_key_id.is_some() {
                return Err(backup_err("missing encryption key"));
            }
            encoded
        };
        let reference = ChunkRef {
            hash: digest,
            len: bytes.len() as u64,
        };
        atomic_write_new_ready(&path, &stored, || self.read_blob(&reference).map(|_| ()))?;
        Ok(reference)
    }
    pub(crate) fn read_blob(&self, reference: &ChunkRef) -> Result<Vec<u8>, QueryError> {
        #[cfg(test)]
        super::faults::hit("blob_read", &self.root)?;
        validate_hash(&reference.hash)?;
        if reference.len == 0 || reference.len > CHUNK_BYTES as u64 {
            return Err(backup_err("WAL blob length exceeds budget"));
        }
        super::resources::throttle(reference.len)?;
        let path = self.blob_path(&reference.hash);
        reject_symlink_path(&self.root, path.strip_prefix(&self.root).unwrap())?;
        let stored = read_bounded(&path, CHUNK_BYTES + 1024)?;
        super::resources::io()
            .blob_read_bytes
            .fetch_add(stored.len() as u64, Ordering::Relaxed);
        super::resources::io()
            .blob_decodes
            .fetch_add(1, Ordering::Relaxed);
        let encoded = if let Some(key) = &self.key {
            if stored.len() < 28 {
                return Err(backup_err("truncated encrypted WAL blob"));
            }
            let aad = format!(
                "{}:{}:{}:{}:{}",
                FORMAT, self.header.id, self.header.wal_encoding, reference.len, reference.hash
            );
            Aes256Gcm::new_from_slice(&key.bytes)
                .map_err(|_| backup_err("invalid WAL key"))?
                .decrypt(
                    Nonce::from_slice(&stored[..12]),
                    Payload {
                        msg: &stored[12..],
                        aad: aad.as_bytes(),
                    },
                )
                .map_err(|_| backup_err("WAL authentication failed"))?
        } else {
            if self.header.wal_key_id.is_some() {
                return Err(backup_err("missing WAL key"));
            }
            stored
        };
        if encoded.len() < 69
            || u64::from_le_bytes(encoded[21..29].try_into().unwrap()) != reference.len
        {
            return Err(backup_err("WAL blob decompression length mismatch"));
        }
        if self.header.wal_encoding == "NONE" && encoded[8] & 1 != 0 {
            return Err(backup_err(
                "compressed object conflicts with NONE WAL encoding",
            ));
        }
        let bytes = decode_blob_expected(&encoded, None, Some(&reference.hash))?;
        if bytes.len() as u64 != reference.len || sha256_hex(&bytes) != reference.hash {
            return Err(backup_err("WAL blob hash/length mismatch"));
        }
        Ok(bytes)
    }
    pub(crate) fn publish_range(&self, range: Range) -> Result<Position, QueryError> {
        validate_id(&range.stream_id)?;
        validate_id(&range.id)?;
        let descriptor = self.stream(&range.stream_id)?;
        if descriptor.source_id != range.source_id
            || descriptor.timeline_id != range.timeline_id
            || range.first_lsn == 0
            || range.first_lsn > range.last_lsn
            || range.frame_count == 0
            || range.raw_bytes == 0
            || range
                .chunks
                .iter()
                .try_fold(0u64, |n, c| n.checked_add(c.len))
                != Some(range.raw_bytes)
        {
            return Err(backup_err("invalid WAL range identity/coverage"));
        }
        let parent = self
            .root
            .join("streams")
            .join(&range.stream_id)
            .join("ranges");
        reject_symlink_path(&self.root, parent.strip_prefix(&self.root).unwrap())?;
        if !parent.is_dir() {
            return Err(backup_err(
                "registered WAL stream ranges directory is missing",
            ));
        }
        let operation =
            DirectoryOperation::new(&self.root.join("staging/range-work"), "range-work")?;
        let bytes = json(&range)?;
        write_sync(&operation.data.join("manifest"), &bytes)?;
        write_sync(&operation.data.join("commit"), &json(&sha256_hex(&bytes))?)?;
        sync_dir(&operation.data)?;
        native::rename_new_directory(&operation.data, &parent.join(&range.id))?;
        sync_dir(&parent)?;
        // Source and export ranges follow a committed baseline. The first
        // catalog publication already confirmed the root's installation.
        self.append_publication(Publication::Range { range }, true)
    }
}

#[cfg(test)]
pub(crate) static FAIL_AFTER_BASE_RENAME: AtomicBool = AtomicBool::new(false);

impl Repository {
    pub(crate) fn record_diagnostic(
        &self,
        position: &Position,
        kind: &str,
        error: Option<String>,
        scope: serde_json::Value,
    ) -> Result<(), QueryError> {
        if !["VERIFY", "RECOVERY_DRILL"].contains(&kind) {
            return Err(backup_err("unknown diagnostic kind"));
        }
        let _publish = self.publish_lock()?;
        let directory = self.root.join("catalog/diagnostics");
        create_directory_durable(&directory)?;
        let payload = serde_json::json!({"version":1,"repository_id":self.header.id,"catalog":position,"kind":kind,"completed_unix_ms":now_ms(),"outcome":if error.is_none(){"PASSED"}else{"FAILED"},"error":error,"scope":scope});
        let digest = sha256_hex(&json(&payload)?);
        atomic_replace(
            &directory.join(format!("{kind}.json")),
            &json(&serde_json::json!({"payload":payload,"digest":digest}))?,
        )
    }
    pub(crate) fn confirm_install(&mut self, key: Option<&Path>) -> Result<(), QueryError> {
        let _lease = Lease::acquire(&self.root, true)?;
        let position = self.position()?;
        if self.needs_wal_key(&position)? {
            self.require_key(key)?;
        }
        self.verify_coverage(&position)?;
        self.verify_history()?;
        self.visit_live(&position, |_, event| {
            match event {
                Publication::Base { base } => {
                    self.base(&base.id)?;
                }
                Publication::Range { range } => super::restore::verify_range(self, &range)?,
                _ => {}
            }
            Ok(())
        })?;
        sync_tree(&self.root)?;
        sync_dir(
            self.root
                .parent()
                .ok_or_else(|| backup_err("repository needs a parent"))?,
        )
    }
}

impl Repository {
    pub(crate) fn needs_wal_key(&self, position: &Position) -> Result<bool, QueryError> {
        if self.header.wal_key_id.is_none() {
            return Ok(false);
        }
        let mut needed = false;
        self.visit_live(position, |_, event| {
            needed |= matches!(event, Publication::Range { .. });
            Ok(())
        })?;
        Ok(needed)
    }
}
