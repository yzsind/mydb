//! Compact, budgeted page inventories. First-round visit ordinals are the
//! directory index, so inverse page permutation replaces an O(P) hash map.
use super::*;
use crate::engine::executor::select::grace::SpillTmpDir;
use crate::engine::executor::spill_quota::{TrackedSpillReader, TrackedSpillWriter};
use std::io::{Read, Seek, SeekFrom, Write};

pub(super) fn io_error(error: std::io::Error) -> QueryError {
    crate::engine::executor::spill_quota::query_error_from_io(&error).unwrap_or_else(|| {
        crate::error::catalog::ER_GET_ERRNO.error(format!("TABLESAMPLE directory I/O: {error}"))
    })
}

#[derive(Clone, Copy, Default)]
pub(super) struct Entry {
    pub storage: u64,
    pub page: u32,
    pub count: u32,
    offset: u64,
}
impl Entry {
    fn bytes(self) -> [u8; 24] {
        let mut bytes = [0; 24];
        bytes[..8].copy_from_slice(&self.storage.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.page.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.count.to_le_bytes());
        bytes[16..].copy_from_slice(&self.offset.to_le_bytes());
        bytes
    }
    fn from_bytes(bytes: [u8; 24]) -> Self {
        Self {
            storage: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
            page: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            count: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            offset: u64::from_le_bytes(bytes[16..].try_into().unwrap()),
        }
    }
}

struct Disk {
    dir: SpillTmpDir,
    index_writer: Option<TrackedSpillWriter>,
    slot_writer: Option<TrackedSpillWriter>,
    index: Option<TrackedSpillReader>,
    slots: Option<TrackedSpillReader>,
}
impl Disk {
    fn new(path: &std::path::Path) -> Result<Self, QueryError> {
        let dir = SpillTmpDir::create_kind(path, "sample")?;
        let index_writer =
            TrackedSpillWriter::create_new(&dir.path().join("index"), dir.charger(), 32768)?;
        let slot_writer =
            TrackedSpillWriter::create_new(&dir.path().join("slots"), dir.charger(), 32768)?;
        Ok(Self {
            dir,
            index_writer: Some(index_writer),
            slot_writer: Some(slot_writer),
            index: None,
            slots: None,
        })
    }
    fn append(&mut self, entry: Entry, slots: &[u16]) -> Result<(), QueryError> {
        self.index_writer
            .as_mut()
            .unwrap()
            .write_all(&entry.bytes())
            .map_err(io_error)?;
        let writer = self.slot_writer.as_mut().unwrap();
        for slot in slots {
            writer.write_all(&slot.to_le_bytes()).map_err(io_error)?;
        }
        Ok(())
    }
    fn seal(&mut self) -> Result<(), QueryError> {
        self.index_writer.take().unwrap().finish()?;
        self.slot_writer.take().unwrap().finish()?;
        self.index = Some(TrackedSpillReader::open(
            &self.dir.path().join("index"),
            self.dir.charger(),
            32768,
        )?);
        self.slots = Some(TrackedSpillReader::open(
            &self.dir.path().join("slots"),
            self.dir.charger(),
            32768,
        )?);
        Ok(())
    }
}

pub(super) struct Store {
    entries: Vec<Entry>,
    slots: Vec<u16>,
    charge: ScopedCharge,
    disk: Option<Disk>,
    path: std::path::PathBuf,
    limit: usize,
    len: u64,
    slot_count: u64,
    pub peak: usize,
    pub spilled: u64,
}
impl Store {
    pub fn new(path: &std::path::Path) -> Self {
        Self {
            entries: Vec::new(),
            slots: Vec::new(),
            charge: ScopedCharge::new(0),
            disk: None,
            path: path.to_owned(),
            limit: mem_budget::current_budget().map_or(8 * 1024 * 1024, |b| {
                (b.limit() / 16).clamp(4096, 8 * 1024 * 1024)
            }),
            len: 0,
            slot_count: 0,
            peak: 0,
            spilled: 0,
        }
    }
    pub fn append(&mut self, task: Task, rows: &RowRefs) -> Result<(), QueryError> {
        let entry = Entry {
            storage: task.storage_id,
            page: task.lo as u32,
            count: u32::try_from(rows.len()).map_err(|_| invalid("root inventory overflow"))?,
            offset: self.slot_count,
        };
        let needed = std::mem::size_of::<Entry>() + rows.len() * 2;
        if self.disk.is_none() && self.charge.charged().saturating_add(needed * 2) > self.limit {
            self.spill()?;
        }
        if self.disk.is_none() {
            // Both vectors grow within a bounded fraction of the query work area.
            if self.entries.len() == self.entries.capacity() {
                let additional = self.entries.capacity().max(8);
                mem_budget::try_reserve_vec_hard(
                    &mut self.entries,
                    additional,
                    &mut self.charge,
                    "sample page index",
                )?;
            }
            if self.disk.is_none() && self.slots.capacity() - self.slots.len() < rows.len() {
                let additional = rows.len().max(self.slots.capacity());
                mem_budget::try_reserve_vec_hard(
                    &mut self.slots,
                    additional,
                    &mut self.charge,
                    "sample root slots",
                )?;
            }
        }
        if let Some(disk) = &mut self.disk {
            disk.index_writer
                .as_mut()
                .unwrap()
                .write_all(&entry.bytes())
                .map_err(io_error)?;
            for row in rows.iter() {
                disk.slot_writer
                    .as_mut()
                    .unwrap()
                    .write_all(&row.row_id.slot_id.to_le_bytes())
                    .map_err(io_error)?;
            }
            self.spilled += 24 + rows.len() as u64 * 2;
        } else {
            self.entries.push(entry);
            self.slots.extend(rows.iter().map(|r| r.row_id.slot_id));
            self.peak = self.peak.max(self.charge.charged());
        }
        self.len += 1;
        self.slot_count = self
            .slot_count
            .checked_add(rows.len() as u64)
            .ok_or_else(|| invalid("root count overflow"))?;
        Ok(())
    }
    fn spill(&mut self) -> Result<(), QueryError> {
        let mut disk = Disk::new(&self.path)?;
        for (i, entry) in self.entries.iter().copied().enumerate() {
            if i % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let start = entry.offset as usize;
            disk.append(entry, &self.slots[start..start + entry.count as usize])?;
        }
        self.spilled += self.entries.len() as u64 * 24 + self.slots.len() as u64 * 2;
        self.entries = Vec::new();
        self.slots = Vec::new();
        self.charge = ScopedCharge::new(0);
        self.disk = Some(disk);
        Ok(())
    }
    pub fn seal(&mut self) -> Result<(), QueryError> {
        if let Some(disk) = &mut self.disk {
            disk.seal()?;
        }
        Ok(())
    }
    pub fn entry(&mut self, index: u64) -> Result<Entry, QueryError> {
        if index >= self.len {
            return Err(invalid("root directory index out of range"));
        }
        if let Some(disk) = &mut self.disk {
            let reader = disk.index.as_mut().unwrap();
            reader
                .seek(SeekFrom::Start(
                    index
                        .checked_mul(24)
                        .ok_or_else(|| invalid("directory offset overflow"))?,
                ))
                .map_err(io_error)?;
            let mut bytes = [0; 24];
            reader.read_exact(&mut bytes).map_err(io_error)?;
            Ok(Entry::from_bytes(bytes))
        } else {
            Ok(self.entries[index as usize])
        }
    }
    pub fn slots(
        &mut self,
        entry: Entry,
        output: &mut Vec<u16>,
        charge: &mut ScopedCharge,
    ) -> Result<(), QueryError> {
        output.clear();
        if output.capacity() < entry.count as usize {
            mem_budget::try_reserve_vec_hard(
                output,
                entry.count as usize,
                charge,
                "sample page replay",
            )?;
        }
        if let Some(disk) = &mut self.disk {
            let reader = disk.slots.as_mut().unwrap();
            reader
                .seek(SeekFrom::Start(
                    entry
                        .offset
                        .checked_mul(2)
                        .ok_or_else(|| invalid("slot offset overflow"))?,
                ))
                .map_err(io_error)?;
            for _ in 0..entry.count {
                let mut bytes = [0; 2];
                reader.read_exact(&mut bytes).map_err(io_error)?;
                output.push(u16::from_le_bytes(bytes));
            }
        } else {
            let start = entry.offset as usize;
            output.extend_from_slice(&self.slots[start..start + entry.count as usize]);
        }
        Ok(())
    }
}
