//! Statement-local physical-row membership, independent of ANN node ordinals.
use crate::engine::vector::workspace::VectorQueryCharge;
use crate::{storage::RowId, types::QueryError};
use std::collections::BTreeMap;

#[derive(Debug)]
enum PartitionRows {
    Sparse(Vec<RowId>),
    Bitmap(BTreeMap<u32, Vec<u64>>),
}
#[derive(Debug)]
pub(crate) struct RowSelection {
    partitions: BTreeMap<u64, PartitionRows>,
    len: usize,
    charge: VectorQueryCharge,
}
impl RowSelection {
    pub(crate) fn new() -> Result<Self, QueryError> {
        Ok(Self {
            partitions: BTreeMap::new(),
            len: 0,
            charge: VectorQueryCharge::try_new_recoverable(0)?,
        })
    }
    pub(crate) fn append(&mut self, sid: u64, ids: &[RowId]) -> Result<(), QueryError> {
        if ids.is_empty() {
            return Ok(());
        }
        self.charge
            .try_grow_recoverable(ids.len().saturating_mul(32) + 128)?;
        let rows = self
            .partitions
            .entry(sid)
            .or_insert_with(|| PartitionRows::Sparse(Vec::new()));
        let PartitionRows::Sparse(rows) = rows else {
            unreachable!("sealed selection");
        };
        rows.extend_from_slice(ids);
        Ok(())
    }
    pub(crate) fn seal(&mut self) -> Result<(), QueryError> {
        self.len = 0;
        for rows in self.partitions.values_mut() {
            let PartitionRows::Sparse(ids) = rows else {
                continue;
            };
            ids.sort_unstable();
            ids.dedup();
            self.len += ids.len();
            let mut bitmap_bytes = 0;
            let mut start = 0;
            while start < ids.len() {
                let end = start + ids[start..].partition_point(|r| r.page_id == ids[start].page_id);
                bitmap_bytes += 64 + (ids[end - 1].slot_id as usize / 64 + 1) * 8;
                start = end;
            }
            if bitmap_bytes < ids.capacity() * std::mem::size_of::<RowId>() {
                self.charge.try_grow_recoverable(bitmap_bytes)?;
                let mut pages = BTreeMap::new();
                for id in ids.iter() {
                    let words = pages.entry(id.page_id).or_insert_with(Vec::new);
                    words.resize(words.len().max(id.slot_id as usize / 64 + 1), 0);
                    words[id.slot_id as usize / 64] |= 1u64 << (id.slot_id % 64);
                }
                *rows = PartitionRows::Bitmap(pages);
            }
        }
        let retained = self
            .partitions
            .values()
            .map(|rows| match rows {
                PartitionRows::Sparse(ids) => 128 + ids.capacity() * std::mem::size_of::<RowId>(),
                PartitionRows::Bitmap(pages) => {
                    128 + pages
                        .values()
                        .map(|words| 64 + words.capacity() * 8)
                        .sum::<usize>()
                }
            })
            .sum::<usize>();
        if retained < self.charge.bytes() {
            self.charge.shrink(self.charge.bytes() - retained);
        }
        Ok(())
    }
    #[inline]
    pub(crate) fn contains(&self, sid: u64, row: RowId) -> bool {
        match self.partitions.get(&sid) {
            Some(PartitionRows::Sparse(ids)) => ids.binary_search(&row).is_ok(),
            Some(PartitionRows::Bitmap(pages)) => pages
                .get(&row.page_id)
                .and_then(|words| words.get(row.slot_id as usize / 64))
                .is_some_and(|word| word & (1u64 << (row.slot_id % 64)) != 0),
            None => false,
        }
    }
    pub(crate) fn len(&self) -> usize {
        self.len
    }
    pub(crate) fn partition_len(&self, sid: u64) -> usize {
        match self.partitions.get(&sid) {
            Some(PartitionRows::Sparse(ids)) => ids.len(),
            Some(PartitionRows::Bitmap(pages)) => pages
                .values()
                .flat_map(|words| words.iter())
                .map(|word| word.count_ones() as usize)
                .sum(),
            None => 0,
        }
    }
    pub(crate) fn visit(
        &self,
        mut f: impl FnMut(u64, &[RowId]) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        for (&sid, rows) in &self.partitions {
            match rows {
                PartitionRows::Sparse(ids) => {
                    for chunk in ids.chunks(256) {
                        f(sid, chunk)?;
                    }
                }
                PartitionRows::Bitmap(pages) => {
                    let _scratch =
                        VectorQueryCharge::try_new_recoverable(256 * std::mem::size_of::<RowId>())?;
                    let mut ids = Vec::with_capacity(256);
                    for (&page, words) in pages {
                        for (i, &word) in words.iter().enumerate() {
                            let mut bits = word;
                            while bits != 0 {
                                let slot = i * 64 + bits.trailing_zeros() as usize;
                                ids.push(RowId::new(page, slot as u16));
                                bits &= bits - 1;
                                if ids.len() == 256 {
                                    f(sid, &ids)?;
                                    ids.clear();
                                }
                            }
                        }
                    }
                    if !ids.is_empty() {
                        f(sid, &ids)?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filtered_vector_selection_preserves_sparse_dense_and_partition_identity() {
        let mut rows = RowSelection::new().unwrap();
        let dense: Vec<_> = (0..1024).map(|i| RowId::new(4, i)).collect();
        rows.append(1, &dense).unwrap();
        rows.append(1, &dense[..5]).unwrap();
        rows.append(2, &[RowId::new(u32::MAX, u16::MAX)]).unwrap();
        rows.seal().unwrap();
        assert_eq!(rows.len(), 1025);
        assert!(matches!(rows.partitions[&1], PartitionRows::Bitmap(_)));
        assert!(matches!(rows.partitions[&2], PartitionRows::Sparse(_)));
        assert!(rows.contains(1, RowId::new(4, 1023)));
        assert!(!rows.contains(2, RowId::new(4, 1023)));
        assert!(rows.contains(2, RowId::new(u32::MAX, u16::MAX)));
        let mut count = 0;
        rows.visit(|sid, ids| {
            for &id in ids {
                assert!(rows.contains(sid, id));
                count += 1;
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1025);
    }
}
