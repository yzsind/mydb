//! Fixed-width external merge ordering of selected identities, never Values
//! or row payloads. Two-way passes bound descriptors and replay buffers.
use super::store::io_error;
use super::*;
use crate::engine::executor::select::grace::SpillTmpDir;
use crate::engine::executor::spill_quota::{self, TrackedSpillReader, TrackedSpillWriter};
use std::io::{Read, Write};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Record {
    pub round: u64,
    pub ordinal: u64,
    pub storage: u64,
    pub row: u64,
}
impl Record {
    fn bytes(self) -> [u8; 32] {
        let mut bytes = [0; 32];
        for (chunk, value) in
            bytes
                .chunks_exact_mut(8)
                .zip([self.round, self.ordinal, self.storage, self.row])
        {
            chunk.copy_from_slice(&value.to_le_bytes());
        }
        bytes
    }
    fn read(reader: &mut TrackedSpillReader) -> Result<Option<Self>, QueryError> {
        if reader.remaining_bytes() == 0 {
            return Ok(None);
        }
        let mut bytes = [0; 32];
        reader.read_exact(&mut bytes).map_err(io_error)?;
        let mut values = bytes
            .chunks_exact(8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()));
        Ok(Some(Self {
            round: values.next().unwrap(),
            ordinal: values.next().unwrap(),
            storage: values.next().unwrap(),
            row: values.next().unwrap(),
        }))
    }
    pub fn identity(self) -> DmlRowRef {
        DmlRowRef {
            storage_id: self.storage,
            row_id: unpack(self.row),
        }
    }
}

pub(super) struct Order {
    rows: Vec<Record>,
    charge: ScopedCharge,
    dir: Option<SpillTmpDir>,
    path: std::path::PathBuf,
    limit: usize,
    runs: u64,
    pub spilled: u64,
    pub peak: usize,
}
impl Order {
    pub fn new(path: &std::path::Path) -> Self {
        Self {
            rows: Vec::new(),
            charge: ScopedCharge::new(0),
            dir: None,
            path: path.to_owned(),
            limit: mem_budget::current_budget().map_or(8 * 1024 * 1024, |b| {
                (b.limit() / 16).clamp(4096, 8 * 1024 * 1024)
            }),
            runs: 0,
            spilled: 0,
            peak: 0,
        }
    }
    pub fn push(&mut self, row: Record) -> Result<(), QueryError> {
        if (self.rows.len() + 1) * std::mem::size_of::<Record>() > self.limit {
            self.flush()?;
        }
        if self.rows.len() == self.rows.capacity() {
            let additional = self.rows.capacity().max(32).min(self.limit / 32);
            mem_budget::try_reserve_vec_hard(
                &mut self.rows,
                additional,
                &mut self.charge,
                "sample ordered identities",
            )?;
            self.peak = self.peak.max(self.charge.charged());
        }
        self.rows.push(row);
        Ok(())
    }
    fn flush(&mut self) -> Result<(), QueryError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        self.rows.sort_unstable();
        if self.dir.is_none() {
            self.dir = Some(SpillTmpDir::create_kind(&self.path, "sort")?);
        }
        let dir = self.dir.as_ref().unwrap();
        let mut writer = TrackedSpillWriter::create_new(
            &dir.path().join(format!("0-{}", self.runs)),
            dir.charger(),
            32768,
        )?;
        for (i, row) in self.rows.iter().enumerate() {
            if i % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            writer.write_all(&row.bytes()).map_err(io_error)?;
        }
        self.spilled += writer.finish()?;
        self.runs += 1;
        self.rows.clear();
        Ok(())
    }
    pub fn finish(mut self) -> Result<(Ordered, u64), QueryError> {
        if self.dir.is_none() {
            self.rows.sort_unstable();
            return Ok((
                Ordered::Resident(self.rows.into_iter(), self.charge),
                self.spilled,
            ));
        }
        self.flush()?;
        self.rows = Vec::new();
        self.charge = ScopedCharge::new(0);
        let dir = self.dir.take().unwrap();
        let mut pass = 0;
        while self.runs > 1 {
            let next_count = self.runs.div_ceil(2);
            for output in 0..next_count {
                crate::engine::cancel::check_cancelled()?;
                let a_path = dir.path().join(format!("{pass}-{}", output * 2));
                let b_path = dir.path().join(format!("{pass}-{}", output * 2 + 1));
                let out_path = dir.path().join(format!("{}-{output}", pass + 1));
                let mut a = TrackedSpillReader::open(&a_path, dir.charger(), 32768)?;
                let mut b = if output * 2 + 1 < self.runs {
                    Some(TrackedSpillReader::open(&b_path, dir.charger(), 32768)?)
                } else {
                    None
                };
                let mut writer = TrackedSpillWriter::create_new(&out_path, dir.charger(), 32768)?;
                let mut av = Record::read(&mut a)?;
                let mut bv = b.as_mut().map(Record::read).transpose()?.flatten();
                let mut count = 0usize;
                while av.is_some() || bv.is_some() {
                    if count % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    count += 1;
                    if av.is_some() && (bv.is_none() || av <= bv) {
                        writer
                            .write_all(&av.take().unwrap().bytes())
                            .map_err(io_error)?;
                        av = Record::read(&mut a)?;
                    } else {
                        writer
                            .write_all(&bv.take().unwrap().bytes())
                            .map_err(io_error)?;
                        bv = Record::read(b.as_mut().unwrap())?;
                    }
                }
                self.spilled += writer.finish()?;
                drop(a);
                drop(b);
                spill_quota::remove_spill_file(&a_path, &dir.charger())?;
                if output * 2 + 1 < self.runs {
                    spill_quota::remove_spill_file(&b_path, &dir.charger())?;
                }
            }
            self.runs = next_count;
            pass += 1;
        }
        let reader =
            TrackedSpillReader::open(&dir.path().join(format!("{pass}-0")), dir.charger(), 32768)?;
        Ok((Ordered::External { reader, _dir: dir }, self.spilled))
    }
}

pub(super) enum Ordered {
    Resident(std::vec::IntoIter<Record>, ScopedCharge),
    External {
        reader: TrackedSpillReader,
        _dir: SpillTmpDir,
    },
}
impl Ordered {
    pub fn next(&mut self) -> Result<Option<DmlRowRef>, QueryError> {
        Ok(match self {
            Self::Resident(rows, _charge) => rows.next(),
            Self::External { reader, .. } => Record::read(reader)?,
        }
        .map(Record::identity))
    }
}
