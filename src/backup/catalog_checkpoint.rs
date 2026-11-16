//! Authenticated live-state checkpoints. Old event chains remain available for
//! external source receipts; ordinary planning reads a checkpoint plus suffix.
use super::*;
use std::io::{Read, Write};
const MAGIC: &[u8; 8] = b"MYDBCC1\n";
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pointer {
    pub(super) version: u32,
    pub(super) repository_id: String,
    pub(super) commit: Position,
    pub(super) anchor: Position,
    pub(super) digest: String,
    pub(super) records: u64,
    pub(super) history: String,
}
#[derive(Debug, Serialize)]
pub(crate) struct CheckpointReport {
    pub anchor: Position,
    pub commit: Position,
    pub records: u64,
    pub bytes: u64,
    pub digest: String,
}
impl Repository {
    pub(crate) fn checkpoint(&self) -> Result<CheckpointReport, QueryError> {
        let _lease = Lease::acquire(&self.root, true)?;
        let _publish = self.publish_lock()?;
        self.checkpoint_locked(&self.position()?)
    }
    pub(super) fn checkpoint_locked(
        &self,
        anchor: &Position,
    ) -> Result<CheckpointReport, QueryError> {
        let operation = DirectoryOperation::new(
            &self.root.join("staging/catalog-checkpoint"),
            "catalog-checkpoint",
        )?;
        let path = operation.data.join("state");
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| io_err("create catalog checkpoint", e))?;
        output
            .write_all(MAGIC)
            .map_err(|e| io_err("write checkpoint magic", e))?;
        let mut digest = Sha256::new();
        digest.update(MAGIC);
        let mut records = 0u64;
        let mut bytes = MAGIC.len() as u64;
        let mut fences = BTreeSet::new();
        let mut closed = BTreeSet::new();
        let mut memory =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)?;
        self.visit_live(anchor, |generation, event| {
            match &event {
                Publication::Checkpoint { .. } | Publication::Prune { .. } => return Ok(()),
                Publication::Fence { stream_id, .. } => {
                    if !fences.insert(stream_id.clone()) {
                        return Ok(());
                    }
                    memory.try_grow(160)?;
                }
                Publication::Close { stream_id, .. } => {
                    if !closed.insert(stream_id.clone()) {
                        return Ok(());
                    }
                    memory.try_grow(160)?;
                }
                _ => {}
            }
            let value = json(&event)?;
            if value.len() > MAX_METADATA_BYTES {
                return Err(backup_err("checkpoint record exceeds metadata limit"));
            }
            bytes = bytes
                .checked_add(value.len() as u64 + 16)
                .ok_or_else(|| backup_err("checkpoint size overflow"))?;
            super::super::resources::check_spool(bytes)?;
            let _disk = reserve_disk_write(&operation.data, value.len() as u64 + 16)?;
            let length = (value.len() as u64).to_le_bytes();
            let generation = generation.to_le_bytes();
            output
                .write_all(&length)
                .and_then(|_| output.write_all(&generation))
                .and_then(|_| output.write_all(&value))
                .map_err(|e| io_err("write checkpoint record", e))?;
            digest.update(length);
            digest.update(generation);
            digest.update(&value);
            records += 1;
            Ok(())
        })?;
        output
            .sync_all()
            .map_err(|e| io_err("sync catalog checkpoint", e))?;
        drop(output);
        let digest = encode_hex(&digest.finalize());
        let directory = self.root.join("catalog/checkpoints");
        create_directory_durable(&directory)?;
        let destination = directory.join(&digest);
        match fs::hard_link(&path, &destination) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let mut input = std::fs::File::open(&destination)
                    .map_err(|e| io_err("verify reused checkpoint", e))?;
                let mut hash = Sha256::new();
                let mut buffer = [0; 65536];
                loop {
                    let count = input
                        .read(&mut buffer)
                        .map_err(|e| io_err("read reused checkpoint", e))?;
                    if count == 0 {
                        break;
                    }
                    hash.update(&buffer[..count]);
                }
                if encode_hex(&hash.finalize()) != digest {
                    return Err(backup_err("conflicting immutable catalog checkpoint"));
                }
            }
            Err(e) => return Err(io_err("publish catalog checkpoint", e)),
        }
        confirm_immutable(&destination)?;
        let history = self.build_history(anchor)?;
        #[cfg(test)]
        super::super::faults::hit("catalog_checkpoint_objects_published", &self.root)?;
        let commit = self.append_locked(
            anchor,
            Publication::Checkpoint {
                anchor: anchor.clone(),
                digest: digest.clone(),
                records,
                history: history.clone(),
            },
        )?;
        #[cfg(test)]
        super::super::faults::hit("catalog_checkpoint_committed", &self.root)?;
        let pointer = Pointer {
            version: crate::storage::format_versions::BACKUP_CHECKPOINT_VERSION,
            repository_id: self.header.id.clone(),
            commit: commit.clone(),
            anchor: anchor.clone(),
            digest: digest.clone(),
            records,
            history,
        };
        atomic_replace(&self.root.join("catalog/CHECKPOINT"), &json(&pointer)?)?;
        #[cfg(test)]
        super::super::faults::hit("catalog_checkpoint_pointer_published", &self.root)?;
        Ok(CheckpointReport {
            anchor: anchor.clone(),
            commit,
            records,
            bytes,
            digest,
        })
    }
    pub(super) fn visit_checkpoint(
        &self,
        position: &Position,
        mut visit: impl FnMut(u64, Publication) -> Result<(), QueryError>,
    ) -> Result<bool, QueryError> {
        let path = self.root.join("catalog/CHECKPOINT");
        reject_symlink_path(&self.root, Path::new("catalog/CHECKPOINT"))?;
        if !path.exists() {
            return Ok(false);
        }
        let pointer: Pointer = read_json(&path)?;
        if pointer.version != crate::storage::format_versions::BACKUP_CHECKPOINT_VERSION
            || pointer.repository_id != self.header.id
            || pointer.anchor.generation.checked_add(1) != Some(pointer.commit.generation)
        {
            return Err(backup_err("invalid checkpoint identity/anchor"));
        }
        validate_hash(&pointer.digest)?;
        if pointer.commit.generation > position.generation {
            return Ok(false);
        }
        let mut current = position.clone();
        while current.generation > pointer.commit.generation {
            let event = self.checked_event(&current)?;
            visit(current.generation, event.publication)?;
            current = Position {
                generation: current.generation - 1,
                digest: event.previous,
            };
        }
        if current != pointer.commit {
            return Err(backup_err("catalog checkpoint is not an ancestor"));
        }
        let event = self.checked_event(&current)?;
        match event.publication {
            Publication::Checkpoint {
                anchor,
                digest,
                records,
                history,
            } if history == pointer.history
                && anchor == pointer.anchor
                && digest == pointer.digest
                && records == pointer.records
                && event.previous == anchor.digest => {}
            _ => return Err(backup_err("checkpoint publication proof mismatch")),
        }
        let path = self.root.join("catalog/checkpoints").join(&pointer.digest);
        reject_symlink_path(&self.root, path.strip_prefix(&self.root).unwrap())?;
        let mut input =
            std::fs::File::open(path).map_err(|e| io_err("open catalog checkpoint", e))?;
        let mut magic = [0; 8];
        input
            .read_exact(&mut magic)
            .map_err(|e| io_err("read checkpoint magic", e))?;
        if &magic != MAGIC {
            return Err(backup_err("invalid checkpoint format"));
        }
        let mut hash = Sha256::new();
        hash.update(magic);
        let mut previous = pointer
            .anchor
            .generation
            .checked_add(1)
            .ok_or_else(|| backup_err("checkpoint generation overflow"))?;
        for _ in 0..pointer.records {
            let mut size = [0; 8];
            let mut generation = [0; 8];
            input
                .read_exact(&mut size)
                .and_then(|_| input.read_exact(&mut generation))
                .map_err(|e| io_err("read checkpoint record", e))?;
            let size_value = u64::from_le_bytes(size);
            let generation_value = u64::from_le_bytes(generation);
            if size_value == 0
                || size_value > MAX_METADATA_BYTES as u64
                || generation_value == 0
                || generation_value >= previous
            {
                return Err(backup_err("invalid checkpoint record bounds/order"));
            }
            previous = generation_value;
            let _memory = ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::SystemMetadata,
                (size_value as usize).saturating_mul(16),
            )?;
            let mut value = vec![0; size_value as usize];
            input
                .read_exact(&mut value)
                .map_err(|e| io_err("read checkpoint payload", e))?;
            hash.update(size);
            hash.update(generation);
            hash.update(&value);
            super::super::resources::io()
                .checkpoint_read_bytes
                .fetch_add(size_value + 16, Ordering::Relaxed);
            let publication: Publication =
                serde_json::from_slice(&value).map_err(|e| backup_err(e.to_string()))?;
            if matches!(
                publication,
                Publication::Checkpoint { .. } | Publication::Prune { .. }
            ) {
                return Err(backup_err("non-live checkpoint record"));
            }
            visit(generation_value, publication)?;
        }
        let mut extra = [0];
        if input
            .read(&mut extra)
            .map_err(|e| io_err("check checkpoint EOF", e))?
            != 0
            || encode_hex(&hash.finalize()) != pointer.digest
        {
            return Err(backup_err("catalog checkpoint digest/EOF mismatch"));
        }
        Ok(true)
    }
    pub(super) fn checked_event(&self, position: &Position) -> Result<Event, QueryError> {
        let bytes = self.event_bytes(position)?;
        let memory = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            bytes.len().saturating_mul(16),
        )?;
        let mut event: Event =
            serde_json::from_slice(&bytes).map_err(|e| backup_err(e.to_string()))?;
        if event.generation != position.generation {
            return Err(backup_err("catalog generation mismatch"));
        }
        event._memory = Some(Arc::new(memory));
        Ok(event)
    }
}
