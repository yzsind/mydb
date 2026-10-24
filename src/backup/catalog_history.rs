//! Immutable packed lineage preserves exact old position proofs after cold
//! generation files are collected. Every index root is catalog-committed.
use super::checkpoint::Pointer;
use super::*;
use std::sync::Arc;
const PACK_MAGIC: &[u8; 8] = b"MYDBCH1\n";
const PACK_TARGET: usize = 4 * 1024 * 1024;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pack {
    first: u64,
    last: u64,
    digest: String,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoryIndex {
    version: u32,
    repository_id: String,
    through: Position,
    packs: Vec<Pack>,
    #[serde(skip)]
    _memory: Option<Arc<ControlledMemoryCharge>>,
}
pub(crate) struct PackBuffer {
    bytes: Vec<u8>,
    _memory: ControlledMemoryCharge,
}
struct PageSummary {
    events: Vec<(u64, String, String)>,
    _memory: ControlledMemoryCharge,
}
impl Repository {
    pub(super) fn build_history(&self, anchor: &Position) -> Result<String, QueryError> {
        let previous = self.history_index()?;
        let through = previous
            .as_ref()
            .map(|i| i.through.clone())
            .unwrap_or_default();
        if through.generation > anchor.generation {
            return Err(backup_err("lineage checkpoint is ahead of catalog"));
        }
        let mut packs = previous
            .as_ref()
            .map(|i| i.packs.clone())
            .unwrap_or_default();
        let mut position = anchor.clone();
        let mut records = Vec::<(u64, Vec<u8>)>::new();
        let mut bytes = 0usize;
        let mut memory =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, 0)?;
        while position.generation > through.generation {
            let raw = self.event_bytes(&position)?;
            let _parse = ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::SystemMetadata,
                raw.len().saturating_mul(16),
            )?;
            let event: Event =
                serde_json::from_slice(&raw).map_err(|e| backup_err(e.to_string()))?;
            if event.generation != position.generation {
                return Err(backup_err("lineage event generation mismatch"));
            }
            let needed = raw.len().saturating_add(32);
            if !records.is_empty()
                && (records.len() >= 1024 || bytes.saturating_add(needed) > PACK_TARGET)
            {
                packs.push(self.write_history_pack(&mut records)?);
                records.clear();
                bytes = 0;
            }
            bytes = bytes
                .checked_add(needed)
                .ok_or_else(|| backup_err("lineage pack size overflow"))?;
            memory.try_grow(bytes.saturating_mul(3).saturating_sub(memory.bytes()))?;
            records.push((position.generation, raw));
            position = Position {
                generation: position.generation - 1,
                digest: event.previous,
            };
        }
        if position != through {
            return Err(backup_err("lineage prefix identity mismatch"));
        }
        if !records.is_empty() {
            packs.push(self.write_history_pack(&mut records)?);
        }
        packs.sort_unstable_by_key(|p| p.first);
        let index = HistoryIndex {
            version: crate::storage::format_versions::BACKUP_LINEAGE_VERSION,
            repository_id: self.header.id.clone(),
            through: anchor.clone(),
            packs,
            _memory: None,
        };
        self.validate_history_index(&index)?;
        let bytes = json(&index)?;
        if bytes.len() > MAX_METADATA_BYTES {
            return Err(backup_err("lineage index exceeds metadata budget"));
        }
        let digest = sha256_hex(&bytes);
        let directory = self.root.join("catalog/history/indexes");
        create_directory_durable(&directory)?;
        let _disk = reserve_disk_write(&self.root, bytes.len() as u64)?;
        atomic_write_new(&directory.join(&digest), &bytes)?;
        Ok(digest)
    }
    fn write_history_pack(&self, records: &mut [(u64, Vec<u8>)]) -> Result<Pack, QueryError> {
        records.reverse();
        let first = records[0].0;
        let last = records.last().unwrap().0;
        if last - first + 1 != records.len() as u64 {
            return Err(backup_err("lineage pack generation gap"));
        }
        let total = records
            .iter()
            .fold(20 + records.len() * 4, |n, (_, bytes)| {
                n.saturating_add(bytes.len())
            });
        if total > MAX_METADATA_BYTES {
            return Err(backup_err("lineage record exceeds pack limit"));
        }
        let mut bytes = Vec::with_capacity(total);
        bytes.extend_from_slice(PACK_MAGIC);
        bytes.extend_from_slice(&first.to_le_bytes());
        bytes.extend_from_slice(&(records.len() as u32).to_le_bytes());
        let mut offset = 20 + records.len() * 4;
        for (_, record) in records.iter() {
            bytes.extend_from_slice(&(offset as u32).to_le_bytes());
            offset += record.len();
        }
        for (_, record) in records.iter() {
            bytes.extend_from_slice(record);
        }
        let encoded = lz4_flex::compress_prepend_size(&bytes);
        let digest = sha256_hex(&encoded);
        let directory = self.root.join("catalog/history/packs");
        create_directory_durable(&directory)?;
        let _disk = reserve_disk_write(&self.root, encoded.len() as u64)?;
        atomic_write_new(&directory.join(&digest), &encoded)?;
        Ok(Pack {
            first,
            last,
            digest,
        })
    }
    fn validate_history_index(&self, index: &HistoryIndex) -> Result<(), QueryError> {
        if index.version != crate::storage::format_versions::BACKUP_LINEAGE_VERSION
            || index.repository_id != self.header.id
        {
            return Err(backup_err("lineage index identity mismatch"));
        }
        let mut next = 1u64;
        for pack in &index.packs {
            validate_hash(&pack.digest)?;
            if pack.first != next || pack.last < pack.first {
                return Err(backup_err("lineage index gap/overlap"));
            }
            next = pack
                .last
                .checked_add(1)
                .ok_or_else(|| backup_err("lineage generation overflow"))?;
        }
        if next
            != index
                .through
                .generation
                .checked_add(1)
                .ok_or_else(|| backup_err("lineage generation overflow"))?
        {
            return Err(backup_err("lineage index coverage mismatch"));
        }
        Ok(())
    }
    pub(super) fn history_pointer(&self) -> Result<Option<Pointer>, QueryError> {
        let relative = Path::new("catalog/CHECKPOINT");
        reject_symlink_path(&self.root, relative)?;
        let path = self.root.join(relative);
        if !path.exists() {
            return Ok(None);
        }
        let pointer: Pointer = read_json(&path)?;
        if pointer.version != crate::storage::format_versions::BACKUP_CHECKPOINT_VERSION
            || pointer.repository_id != self.header.id
            || pointer.commit.generation
                != pointer
                    .anchor
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| backup_err("checkpoint generation overflow"))?
        {
            return Err(backup_err("checkpoint history identity mismatch"));
        }
        validate_hash(&pointer.history)?;
        let path = self.root.join(format!(
            "catalog/generations/{:020}-{}",
            pointer.commit.generation, pointer.commit.digest
        ));
        reject_symlink_path(&self.root, path.strip_prefix(&self.root).unwrap())?;
        let bytes = read_bounded(&path, MAX_METADATA_BYTES)?;
        if sha256_hex(&bytes) != pointer.commit.digest {
            return Err(backup_err("lineage catalog commit hash mismatch"));
        }
        let event: Event = serde_json::from_slice(&bytes).map_err(|e| backup_err(e.to_string()))?;
        match event.publication {
            Publication::Checkpoint {
                anchor,
                history,
                digest,
                records,
            } if anchor == pointer.anchor
                && history == pointer.history
                && digest == pointer.digest
                && records == pointer.records
                && event.previous == anchor.digest => {}
            _ => return Err(backup_err("lineage root is not catalog-authenticated")),
        }
        Ok(Some(pointer))
    }
    pub(super) fn history_index(&self) -> Result<Option<Arc<HistoryIndex>>, QueryError> {
        let Some(pointer) = self.history_pointer()? else {
            return Ok(None);
        };
        if let Some((digest, index)) = self
            .history_index_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .filter(|(digest, _)| *digest == pointer.history)
        {
            let _ = digest;
            return Ok(Some(index.clone()));
        }
        let path = self
            .root
            .join("catalog/history/indexes")
            .join(&pointer.history);
        reject_symlink_path(&self.root, path.strip_prefix(&self.root).unwrap())?;
        let bytes = read_bounded(&path, MAX_METADATA_BYTES)?;
        super::super::resources::io()
            .lineage_read_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        if sha256_hex(&bytes) != pointer.history {
            return Err(backup_err("lineage index checksum mismatch"));
        }
        let memory = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            bytes.len().saturating_mul(16),
        )?;
        let mut index: HistoryIndex =
            serde_json::from_slice(&bytes).map_err(|e| backup_err(e.to_string()))?;
        self.validate_history_index(&index)?;
        if index.through != pointer.anchor {
            return Err(backup_err("lineage index anchor mismatch"));
        }
        index._memory = Some(Arc::new(memory));
        let index = Arc::new(index);
        *self
            .history_index_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some((pointer.history, index.clone()));
        Ok(Some(index))
    }
    pub(super) fn packed_event_bytes(&self, position: &Position) -> Result<Vec<u8>, QueryError> {
        let cached = self
            .history_index_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .filter(|(_, index)| index.through.generation >= position.generation)
            .map(|(_, index)| index.clone());
        let index = if let Some(index) = cached {
            index
        } else {
            self.history_index()?.ok_or_else(|| {
                backup_err("catalog generation is missing and has no packed proof")
            })?
        };
        let i = index
            .packs
            .partition_point(|p| p.last < position.generation);
        let pack = index
            .packs
            .get(i)
            .filter(|p| p.first <= position.generation)
            .ok_or_else(|| backup_err("catalog generation is outside packed lineage"))?;
        let cached = self
            .history_pack_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .filter(|(hash, _)| *hash == pack.digest)
            .map(|(_, buffer)| buffer.clone());
        let buffer = if let Some(buffer) = cached {
            buffer
        } else {
            let path = self.root.join("catalog/history/packs").join(&pack.digest);
            reject_symlink_path(&self.root, path.strip_prefix(&self.root).unwrap())?;
            let encoded = read_bounded(&path, MAX_METADATA_BYTES + 1024 * 1024)?;
            super::super::resources::io()
                .lineage_read_bytes
                .fetch_add(encoded.len() as u64, Ordering::Relaxed);
            if sha256_hex(&encoded) != pack.digest || encoded.len() < 4 {
                return Err(backup_err("lineage pack hash/length mismatch"));
            }
            let len = u32::from_le_bytes(encoded[..4].try_into().unwrap()) as usize;
            if len > MAX_METADATA_BYTES || len < 20 {
                return Err(backup_err("lineage pack decoded size exceeds budget"));
            }
            let memory = ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::StorageScratch,
                len.saturating_mul(2).saturating_add(encoded.len()),
            )?;
            let bytes = lz4_flex::decompress_size_prepended(&encoded)
                .map_err(|e| backup_err(format!("invalid lineage encoding: {e}")))?;
            if bytes.len() != len {
                return Err(backup_err("lineage decoded length mismatch"));
            }
            let buffer = Arc::new(PackBuffer {
                bytes,
                _memory: memory,
            });
            *self
                .history_pack_cache
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some((pack.digest.clone(), buffer.clone()));
            buffer
        };
        let bytes = &buffer.bytes;
        if &bytes[..8] != PACK_MAGIC
            || u64::from_le_bytes(bytes[8..16].try_into().unwrap()) != pack.first
        {
            return Err(backup_err("lineage pack generation identity mismatch"));
        }
        if bytes.len() < 20 {
            return Err(backup_err("short lineage page"));
        }
        let count = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        if count == 0
            || count as u64 != pack.last - pack.first + 1
            || count > (bytes.len() - 20) / 4
        {
            return Err(backup_err("invalid lineage offset table"));
        }
        let n = (position.generation - pack.first) as usize;
        let start = u32::from_le_bytes(bytes[20 + n * 4..24 + n * 4].try_into().unwrap()) as usize;
        let end = if n + 1 < count {
            u32::from_le_bytes(bytes[24 + n * 4..28 + n * 4].try_into().unwrap()) as usize
        } else {
            bytes.len()
        };
        if start < 20 + count * 4 || end <= start || end > bytes.len() {
            return Err(backup_err("invalid lineage event offsets"));
        }
        let value = bytes[start..end].to_vec();
        if sha256_hex(&value) != position.digest {
            return Err(backup_err("old catalog receipt is not in packed lineage"));
        }
        Ok(value)
    }
    pub(super) fn event_bytes(&self, position: &Position) -> Result<Vec<u8>, QueryError> {
        validate_hash(&position.digest)?;
        let path = self.root.join(format!(
            "catalog/generations/{:020}-{}",
            position.generation, position.digest
        ));
        reject_symlink_path(&self.root, path.strip_prefix(&self.root).unwrap())?;
        let bytes = if path.exists() {
            let bytes = read_bounded(&path, MAX_METADATA_BYTES)?;
            super::super::resources::io()
                .catalog_read_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            bytes
        } else {
            self.packed_event_bytes(position)?
        };
        if sha256_hex(&bytes) != position.digest {
            return Err(backup_err("catalog hash-chain mismatch"));
        }
        Ok(bytes)
    }
    fn pack_range_events(&self, pack: &Pack) -> Result<PageSummary, QueryError> {
        let path = self.root.join("catalog/history/packs").join(&pack.digest);
        reject_symlink_path(&self.root, path.strip_prefix(&self.root).unwrap())?;
        let encoded = read_bounded(&path, MAX_METADATA_BYTES + 1024 * 1024)?;
        super::super::resources::io()
            .lineage_read_bytes
            .fetch_add(encoded.len() as u64, Ordering::Relaxed);
        if sha256_hex(&encoded) != pack.digest || encoded.len() < 4 {
            return Err(backup_err("lineage pack digest mismatch"));
        }
        let len = u32::from_le_bytes(encoded[..4].try_into().unwrap()) as usize;
        if len > MAX_METADATA_BYTES || len < 20 {
            return Err(backup_err("invalid lineage allocation bound"));
        }
        let _memory = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::StorageScratch,
            len.saturating_mul(3).saturating_add(encoded.len()),
        )?;
        let bytes =
            lz4_flex::decompress_size_prepended(&encoded).map_err(|e| backup_err(e.to_string()))?;
        if bytes.len() != len {
            return Err(backup_err("lineage decoded length mismatch"));
        }
        let count = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        if &bytes[..8] != PACK_MAGIC
            || u64::from_le_bytes(bytes[8..16].try_into().unwrap()) != pack.first
            || count == 0
            || count as u64 != pack.last - pack.first + 1
            || count > (bytes.len() - 20) / 4
        {
            return Err(backup_err("invalid lineage page"));
        }
        let mut result = Vec::with_capacity(count);
        for n in 0..count {
            let start =
                u32::from_le_bytes(bytes[20 + n * 4..24 + n * 4].try_into().unwrap()) as usize;
            let end = if n + 1 < count {
                u32::from_le_bytes(bytes[24 + n * 4..28 + n * 4].try_into().unwrap()) as usize
            } else {
                bytes.len()
            };
            if start < 20 + count * 4 || end <= start || end > bytes.len() {
                return Err(backup_err("invalid lineage offsets"));
            }
            let memory = ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::SystemMetadata,
                (end - start).saturating_mul(16),
            )?;
            let event: Event = serde_json::from_slice(&bytes[start..end])
                .map_err(|e| backup_err(e.to_string()))?;
            if event.generation != pack.first + n as u64 {
                return Err(backup_err("lineage event generation mismatch"));
            }
            drop(memory);
            result.push((
                pack.first + n as u64,
                sha256_hex(&bytes[start..end]),
                event.previous,
            ));
        }
        Ok(PageSummary {
            events: result,
            _memory,
        })
    }
    pub(super) fn visit_since_checkpoint(
        &self,
        position: &Position,
        after: &Position,
        mut visit: impl FnMut(Publication) -> Result<(), QueryError>,
    ) -> Result<bool, QueryError> {
        let Some(pointer) = self.history_pointer()? else {
            return Ok(false);
        };
        if position.generation < pointer.commit.generation
            || after.generation > pointer.anchor.generation
        {
            return Ok(false);
        }
        if after.generation == 0 {
            if !after.digest.is_empty() {
                return Err(backup_err("invalid catalog genesis receipt"));
            }
        } else {
            self.packed_event_bytes(after)?;
        }
        self.visit_checkpoint(position, |generation, event| {
            if generation > after.generation {
                visit(event)?;
            }
            Ok(())
        })
    }
    pub(crate) fn gc_history(&self) -> Result<u64, QueryError> {
        let _lease = Lease::acquire(&self.root, true)?;
        self.gc_history_locked()
    }
    pub(crate) fn gc_history_locked(&self) -> Result<u64, QueryError> {
        let _publish = self.publish_lock()?;
        let Some(index) = self.history_index()? else {
            return Ok(0);
        };
        let pointer = self
            .history_pointer()?
            .ok_or_else(|| backup_err("missing current history proof"))?;
        let stamp_path = self.root.join("catalog/HISTORY_GC");
        reject_symlink_path(&self.root, Path::new("catalog/HISTORY_GC"))?;
        let stamp = if stamp_path.exists() {
            Some(read_json::<Position>(&stamp_path)?)
        } else {
            None
        };
        let minimum = if let Some(stamp) = &stamp {
            if stamp == &pointer.commit {
                u64::MAX
            } else {
                if stamp.generation >= pointer.commit.generation {
                    return Err(backup_err("history GC receipt is ahead of catalog"));
                }
                self.packed_event_bytes(stamp)?;
                stamp.generation
            }
        } else {
            1
        };
        let mut deleted = 0u64;
        let directory = self.root.join("catalog/generations");
        // Delete in pack order so one bounded cache serves every event in a
        // page. Filesystem enumeration order must not cause pack thrashing.
        for pack in index.packs.iter().filter(|pack| pack.last >= minimum) {
            let raw = self.pack_range_events(pack)?;
            for (generation, digest, _) in raw
                .events
                .iter()
                .filter(|(generation, _, _)| *generation >= minimum)
            {
                let path = directory.join(format!("{generation:020}-{digest}"));
                match fs::symlink_metadata(&path) {
                    Ok(metadata) if metadata.file_type().is_file() => {
                        #[cfg(test)]
                        super::super::faults::hit("catalog_history_before_delete", &self.root)?;
                        fs::remove_file(&path)
                            .map_err(|e| io_err("remove packed catalog generation", e))?;
                        deleted += 1;
                        #[cfg(test)]
                        super::super::faults::hit("catalog_history_after_delete", &self.root)?;
                    }
                    Ok(_) => return Err(backup_err("unexpected cold generation file type")),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(io_err("inspect cold generation", error)),
                }
            }
        }
        // Unknown or uncommitted branch files are diagnosed, never silently
        // mistaken for the current lineage.
        for entry in fs::read_dir(&directory).map_err(|e| io_err("inspect remaining catalog", e))? {
            let entry = entry.map_err(|e| io_err("read remaining catalog", e))?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| backup_err("unknown catalog object"))?;
            let Some((generation, digest)) = name.split_once('-') else {
                return Err(backup_err("unknown catalog object"));
            };
            let generation = generation
                .parse::<u64>()
                .ok()
                .filter(|_| generation.len() == 20)
                .ok_or_else(|| backup_err("invalid catalog object generation"))?;
            if super::super::gc::temporary_blob_name(digest) {
                if !entry
                    .file_type()
                    .map_err(|e| io_err("inspect catalog temporary", e))?
                    .is_file()
                    || entry
                        .metadata()
                        .map_err(|e| io_err("size catalog temporary", e))?
                        .len()
                        > MAX_METADATA_BYTES as u64
                {
                    return Err(backup_err("invalid catalog temporary object"));
                }
                fs::remove_file(entry.path()).map_err(|e| io_err("remove catalog temporary", e))?;
                deleted += 1;
                continue;
            }
            validate_hash(digest)?;
            if generation <= index.through.generation {
                self.packed_event_bytes(&Position {
                    generation,
                    digest: digest.into(),
                })?;
                if !entry
                    .file_type()
                    .map_err(|e| io_err("inspect restored cold object", e))?
                    .is_file()
                {
                    return Err(backup_err("invalid cold catalog object type"));
                }
                fs::remove_file(entry.path())
                    .map_err(|e| io_err("remove restored cold object", e))?;
                deleted += 1;
            }
        }

        let pointer = self
            .history_pointer()?
            .ok_or_else(|| backup_err("missing catalog checkpoint"))?;
        let live_packs = index
            .packs
            .iter()
            .map(|p| p.digest.as_str())
            .collect::<BTreeSet<_>>();
        for (relative, keep) in [
            (
                "catalog/checkpoints",
                BTreeSet::from([pointer.digest.as_str()]),
            ),
            (
                "catalog/history/indexes",
                BTreeSet::from([pointer.history.as_str()]),
            ),
            ("catalog/history/packs", live_packs),
        ] {
            let directory = self.root.join(relative);
            if !directory.exists() {
                continue;
            }
            let mut dirty = false;
            for entry in
                fs::read_dir(&directory).map_err(|e| io_err("inspect catalog objects", e))?
            {
                let entry = entry.map_err(|e| io_err("read catalog object", e))?;
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| backup_err("unknown catalog object"))?;
                if keep.contains(name.as_str()) {
                    continue;
                }
                if !super::super::gc::temporary_blob_name(&name) {
                    validate_hash(&name)?;
                }
                if !entry
                    .file_type()
                    .map_err(|e| io_err("inspect catalog object", e))?
                    .is_file()
                {
                    return Err(backup_err("unknown catalog object type"));
                }
                fs::remove_file(entry.path())
                    .map_err(|e| io_err("remove obsolete catalog object", e))?;
                dirty = true;
            }
            if dirty {
                sync_dir(&directory)?;
            }
        }
        if deleted > 0 {
            sync_dir(&directory)?;
        }
        if stamp.as_ref() != Some(&pointer.commit) {
            atomic_replace(&stamp_path, &json(&pointer.commit)?)?;
        }
        Ok(deleted)
    }
}

impl Repository {
    pub(crate) fn verify_history(&self) -> Result<(), QueryError> {
        let Some(index) = self.history_index()? else {
            return Ok(());
        };
        let mut previous = String::new();
        let mut generation = 0u64;
        for pack in &index.packs {
            let page = self.pack_range_events(pack)?;
            for (current, digest, parent) in &page.events {
                if *current != generation + 1 || *parent != previous {
                    return Err(backup_err("packed catalog hash-chain mismatch"));
                }
                generation = *current;
                previous = digest.clone();
            }
        }
        if generation != index.through.generation || previous != index.through.digest {
            return Err(backup_err("packed catalog anchor mismatch"));
        }
        Ok(())
    }
}

#[cfg(test)]
impl Repository {
    /// Seed a hash-linked history for compaction/GC tests. These tests exercise
    /// real checkpoint publication and reopening, not per-event crash barriers.
    /// Keep the production pack boundaries while publishing CURRENT only at
    /// those boundaries and at the end of fixture construction.
    pub(crate) fn test_seed_loose_fences(
        &self,
        stream: &str,
        lsn: u64,
        time: u64,
        events: u64,
    ) -> Result<(), QueryError> {
        let _lease = Lease::acquire(&self.root, true)?;
        let _publish = self.publish_lock()?;
        let mut position = self.position()?;
        for n in 1..=events {
            let event = Event {
                _memory: None,
                generation: position.generation + 1,
                previous: position.digest,
                publication: Publication::Fence {
                    stream_id: stream.into(),
                    lsn,
                    time: time + n,
                },
            };
            let bytes = json(&event)?;
            position = Position {
                generation: event.generation,
                digest: sha256_hex(&bytes),
            };
            let path = self.root.join(format!(
                "catalog/generations/{:020}-{}",
                position.generation, position.digest
            ));
            fs::write(path, bytes).map_err(|e| io_err("seed catalog history", e))?;
            if position.generation % 1024 == 0 {
                atomic_replace(&self.root.join("catalog/CURRENT"), &json(&position)?)?;
                position = self.checkpoint_locked(&position)?.commit;
            }
        }
        atomic_replace(&self.root.join("catalog/CURRENT"), &json(&position)?)
    }

    /// Build a realistic packed metadata corpus without materializing hundreds
    /// of thousands of redundant loose filesystem objects in the test runner.
    pub(crate) fn test_seed_packed_year(
        &self,
        stream: &str,
        lsn: u64,
        time: u64,
        events: u64,
    ) -> Result<(Position, Position), QueryError> {
        let _lease = Lease::acquire(&self.root, true)?;
        let _publish = self.publish_lock()?;
        let original = self.position()?;
        let mut position = original.clone();
        let mut records = Vec::new();
        let mut packs = Vec::new();
        self.visit(&original, |generation, event| {
            let bytes = json(&Event {
                _memory: None,
                generation,
                previous: if generation == 1 {
                    String::new()
                } else {
                    return Err(backup_err("year fixture requires one initial baseline"));
                },
                publication: event,
            })?;
            records.push((generation, bytes));
            Ok(())
        })?;
        let mut middle = original.clone();
        for n in 1..=events {
            let event = Event {
                _memory: None,
                generation: position.generation + 1,
                previous: position.digest,
                publication: Publication::Fence {
                    stream_id: stream.into(),
                    lsn,
                    time: time + n * 60_000_000,
                },
            };
            let bytes = json(&event)?;
            position = Position {
                generation: event.generation,
                digest: sha256_hex(&bytes),
            };
            records.push((position.generation, bytes));
            if n == events / 2 {
                middle = position.clone();
            }
            if records.len() == 1024 {
                records.reverse();
                packs.push(self.write_history_pack(&mut records)?);
                records.clear();
            }
        }
        if !records.is_empty() {
            records.reverse();
            packs.push(self.write_history_pack(&mut records)?);
        }
        let index = HistoryIndex {
            version: crate::storage::format_versions::BACKUP_LINEAGE_VERSION,
            repository_id: self.header.id.clone(),
            through: position.clone(),
            packs,
            _memory: None,
        };
        self.validate_history_index(&index)?;
        let value = json(&index)?;
        let history = sha256_hex(&value);
        atomic_write_new(
            &self.root.join("catalog/history/indexes").join(&history),
            &value,
        )?;
        // Install a temporary valid pointer so checkpoint construction can use
        // the packed prefix with the exact original generation digests.
        let commit = self.append_locked(
            &position,
            Publication::Checkpoint {
                anchor: position.clone(),
                digest: sha256_hex(b"unused"),
                records: 0,
                history: history.clone(),
            },
        )?;
        let pointer = Pointer {
            version: crate::storage::format_versions::BACKUP_CHECKPOINT_VERSION,
            repository_id: self.header.id.clone(),
            commit: commit.clone(),
            anchor: position,
            digest: sha256_hex(b"unused"),
            records: 0,
            history,
        };
        atomic_replace(&self.root.join("catalog/CHECKPOINT"), &json(&pointer)?)?;
        // Build the final two-record state directly; there is no previous live
        // state file in this synthetic setup, only the fully verified lineage.
        let base_event = self.checked_event(&original)?;
        let fence = self.checked_event(&Position {
            generation: commit.generation - 1,
            digest: self.history_index()?.unwrap().through.digest.clone(),
        })?;
        let mut state = Vec::new();
        state.extend_from_slice(b"MYDBCC1\n");
        for (generation, event) in [
            (fence.generation, fence.publication),
            (original.generation, base_event.publication),
        ] {
            let bytes = json(&event)?;
            state.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            state.extend_from_slice(&generation.to_le_bytes());
            state.extend_from_slice(&bytes);
        }
        let digest = sha256_hex(&state);
        atomic_write_new(&self.root.join("catalog/checkpoints").join(&digest), &state)?;
        let history = self.build_history(&commit)?;
        let final_commit = self.append_locked(
            &commit,
            Publication::Checkpoint {
                anchor: commit.clone(),
                digest: digest.clone(),
                records: 2,
                history: history.clone(),
            },
        )?;
        let pointer = Pointer {
            version: crate::storage::format_versions::BACKUP_CHECKPOINT_VERSION,
            repository_id: self.header.id.clone(),
            commit: final_commit.clone(),
            anchor: commit,
            digest,
            records: 2,
            history,
        };
        atomic_replace(&self.root.join("catalog/CHECKPOINT"), &json(&pointer)?)?;
        Ok((middle, final_commit))
    }
}
