//! Durable, idempotent physical sweeping after catalog reachability changes.
use super::repository::{Position, Publication, Range, Repository};
use super::*;

const PENDING: &str = "catalog/GC_PENDING";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    version: u32,
    repository_id: String,
    anchor: Position,
    paths: Vec<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    payload: Plan,
    digest: String,
}
pub(super) struct Garbage {
    pub paths: BTreeSet<String>,
    memory: ControlledMemoryCharge,
}
pub(super) fn temporary_blob_name(name: &str) -> bool {
    name.split_once(".tmp-").is_some_and(|(hash, id)| {
        validate_hash(hash).is_ok()
            && crate::util::uuid::parse(id)
                .is_some_and(|uuid| crate::util::uuid::format(&uuid) == id)
    })
}
impl Garbage {
    fn new() -> Result<Self, QueryError> {
        Ok(Self {
            paths: BTreeSet::new(),
            memory: ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)?,
        })
    }
    pub fn insert(&mut self, path: String) -> Result<(), QueryError> {
        if !self.paths.contains(&path) {
            if self.paths.len() >= 100_000 {
                return Err(backup_err("physical GC plan exceeds budget"));
            }
            self.memory.try_grow(path.len().saturating_add(160))?;
            self.paths.insert(path);
        }
        Ok(())
    }
}
fn validate_path(repo: &Repository, path: &str) -> Result<(), QueryError> {
    let parts = path.split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        ["bases", id] => validate_id(id)?,
        ["streams", stream, "ranges", id] => {
            validate_id(stream)?;
            validate_id(id)?;
        }
        ["blobs", namespace, hash]
            if *namespace == repo.header.wal_key_id.as_deref().unwrap_or("plain") =>
        {
            if !temporary_blob_name(hash) {
                validate_hash(hash)?;
            }
        }
        _ => return Err(backup_err("invalid physical GC object path")),
    }
    reject_symlink_path(&repo.root, Path::new(path))
}
fn bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, QueryError> {
    serde_json::to_vec(value).map_err(|e| backup_err(e.to_string()))
}

pub(super) fn candidates(
    repo: &Repository,
    position: &Position,
    referenced_blobs: &BTreeSet<String>,
) -> Result<Garbage, QueryError> {
    let mut live = BTreeSet::new();
    let mut memory = ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)?;
    repo.visit_live(position, |_, event| {
        let path = match event {
            Publication::Base { base } => Some(format!("bases/{}", base.id)),
            Publication::Range { range } => {
                Some(format!("streams/{}/ranges/{}", range.stream_id, range.id))
            }
            _ => None,
        };
        if let Some(path) = path {
            memory.try_grow(path.len() + 160)?;
            live.insert(path);
        }
        Ok(())
    })?;
    let mut garbage = Garbage::new()?;
    let pending = repo.root.join(PENDING);
    reject_symlink_path(&repo.root, Path::new(PENDING))?;
    if pending.exists() {
        let input = read_bounded(&pending, MAX_METADATA_BYTES)?;
        let _parse = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            input.len().saturating_mul(16),
        )?;
        let envelope: Envelope = serde_json::from_slice(&input)
            .map_err(|e| backup_err(format!("invalid pending GC plan: {e}")))?;
        if envelope.payload.version != 1
            || envelope.payload.repository_id != repo.header.id
            || sha256_hex(&bytes(&envelope.payload)?) != envelope.digest
        {
            return Err(backup_err("GC plan identity/checksum mismatch"));
        }
        repo.visit_since(position, &envelope.payload.anchor, |_| Ok(()))?;
        let blob_prefix = format!(
            "blobs/{}/",
            repo.header.wal_key_id.as_deref().unwrap_or("plain")
        );
        for path in envelope.payload.paths {
            validate_path(repo, &path)?;
            // A writer may legitimately reuse an orphan blob after a failed
            // sweep released its lease. The new root cancels that deletion.
            let reachable = live.contains(&path)
                || path
                    .strip_prefix(&blob_prefix)
                    .is_some_and(|hash| referenced_blobs.contains(hash));
            if !reachable {
                garbage.insert(path)?;
            }
        }
    }
    // Normal sweeps use the current checkpoint and persistent GC plan. Only
    // an incomplete, unplanned orphan needs the cold historical owner proof.
    for entry in
        fs::read_dir(repo.root.join("bases")).map_err(|e| io_err("find orphan bases", e))?
    {
        let entry = entry.map_err(|e| io_err("read orphan base", e))?;
        let id = entry
            .file_name()
            .into_string()
            .map_err(|_| backup_err("non-UTF8 base name"))?;
        let path = format!("bases/{id}");
        validate_path(repo, &path)?;
        if !live.contains(&path) && !garbage.paths.contains(&path) {
            if let Err(error) = repo.base(&id) {
                if !historically_owned(repo, position, &path)? {
                    return Err(error);
                }
            }
            garbage.insert(path)?;
        }
    }
    for stream in
        fs::read_dir(repo.root.join("streams")).map_err(|e| io_err("find orphan ranges", e))?
    {
        let stream = stream.map_err(|e| io_err("read orphan stream", e))?;
        let id = stream
            .file_name()
            .into_string()
            .map_err(|_| backup_err("non-UTF8 stream name"))?;
        validate_id(&id)?;
        reject_symlink_path(&repo.root, &PathBuf::from("streams").join(&id))?;
        let ranges = stream.path().join("ranges");
        if !ranges.exists() {
            continue;
        }
        reject_symlink_path(&repo.root, ranges.strip_prefix(&repo.root).unwrap())?;
        for entry in fs::read_dir(ranges).map_err(|e| io_err("enumerate orphan ranges", e))? {
            let entry = entry.map_err(|e| io_err("read orphan range", e))?;
            let range_id = entry
                .file_name()
                .into_string()
                .map_err(|_| backup_err("non-UTF8 range name"))?;
            let path = format!("streams/{id}/ranges/{range_id}");
            validate_path(repo, &path)?;
            if !live.contains(&path) && !garbage.paths.contains(&path) {
                let verified = (|| -> Result<(), QueryError> {
                    let range: Range = serde_json::from_slice(&read_bounded(
                        &entry.path().join("manifest"),
                        MAX_METADATA_BYTES,
                    )?)
                    .map_err(|e| backup_err(e.to_string()))?;
                    if range.id != range_id || range.stream_id != id {
                        return Err(backup_err("orphan range identity mismatch"));
                    }
                    super::restore::verify_range(repo, &range)?;
                    Ok(())
                })();
                if let Err(error) = verified {
                    if !historically_owned(repo, position, &path)? {
                        return Err(error);
                    }
                }
                garbage.insert(path)?;
            }
        }
    }
    Ok(garbage)
}

pub(super) fn sweep(repo: &Repository, garbage: Garbage) -> Result<(), QueryError> {
    if garbage.paths.is_empty() {
        match fs::remove_file(repo.root.join(PENDING)) {
            Ok(()) => sync_dir(&repo.root.join("catalog"))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err("complete empty physical GC", e)),
        }
        return Ok(());
    }
    let publication_bytes = garbage.paths.iter().fold(512usize, |n, path| {
        n.saturating_add(path.len().saturating_mul(3))
            .saturating_add(64)
    });
    let _publication = ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::SystemMetadata,
        publication_bytes,
    )?;
    let payload = Plan {
        version: 1,
        repository_id: repo.header.id.clone(),
        anchor: repo.position()?,
        paths: garbage.paths.iter().cloned().collect(),
    };
    let digest = sha256_hex(&bytes(&payload)?);
    atomic_replace(
        &repo.root.join(PENDING),
        &bytes(&Envelope { payload, digest })?,
    )?;
    let mut dirty = BTreeSet::new();
    for relative in &garbage.paths {
        validate_path(repo, relative)?;
        let path = repo.root.join(relative);
        dirty.insert(path.parent().unwrap().to_path_buf());
        #[cfg(test)]
        super::faults::hit("gc_before_delete", &path)?;
        let result = if relative.starts_with("blobs/") {
            fs::remove_file(&path)
        } else {
            fs::remove_dir_all(&path)
        };
        match result {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err("sweep backup object", e)),
        }
        #[cfg(test)]
        super::faults::hit("gc_after_delete", &path)?;
    }
    for path in dirty {
        sync_dir(&path)?;
    }
    fs::remove_file(repo.root.join(PENDING)).map_err(|e| io_err("complete physical GC", e))?;
    sync_dir(&repo.root.join("catalog"))
}

fn historically_owned(
    repo: &Repository,
    position: &Position,
    path: &str,
) -> Result<bool, QueryError> {
    let mut owned = false;
    repo.visit(position, |_, event| {
        let published = match event {
            Publication::Base { base } => Some(format!("bases/{}", base.id)),
            Publication::Range { range } => {
                Some(format!("streams/{}/ranges/{}", range.stream_id, range.id))
            }
            _ => None,
        };
        owned |= published.as_deref() == Some(path);
        Ok(())
    })?;
    Ok(owned)
}
