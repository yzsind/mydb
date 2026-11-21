//! Statement-local physical-row membership, independent of ANN node ordinals.
use crate::engine::vector::workspace::VectorQueryCharge;
use crate::{storage::RowId, types::QueryError};
use std::collections::BTreeMap;

#[derive(Debug)]
enum PartitionRows {
    Sparse(Vec<RowId>),
    Bitmap(BTreeMap<u32, Vec<u64>>),
    DensePages {
        first: u32,
        stride: usize,
        words: Vec<u64>,
        count: usize,
    },
}
#[derive(Debug)]
pub(crate) struct RowSelection {
    partitions: BTreeMap<u64, PartitionRows>,
    len: usize,
    charge: VectorQueryCharge,
}
/// A shallow connector search is useful when it avoids substantial scoring
/// and the expected direct graph degree can still support navigation.
pub(crate) fn use_predicate_navigation(matching: usize, population: usize, degree: usize) -> bool {
    matching.saturating_mul(degree) >= population.saturating_mul(2)
        && matching.saturating_mul(8) <= population.saturating_mul(7)
        && matching > 0
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
            let sparse_bytes = ids.capacity() * std::mem::size_of::<RowId>();
            if let (Some(first), Some(last)) = (ids.first(), ids.last()) {
                let first = first.page_id;
                let span = ((last.page_id - first) as usize).saturating_add(1);
                let stride = ids
                    .iter()
                    .map(|id| id.slot_id as usize / 64 + 1)
                    .max()
                    .unwrap_or(1);
                let dense_bytes = span.saturating_mul(stride).saturating_mul(8);
                if span > 1 && dense_bytes <= bitmap_bytes.min(sparse_bytes) {
                    self.charge.try_grow_recoverable(dense_bytes)?;
                    let mut words = vec![0; span * stride];
                    for id in ids.iter() {
                        let index =
                            (id.page_id - first) as usize * stride + id.slot_id as usize / 64;
                        words[index] |= 1u64 << (id.slot_id % 64);
                    }
                    *rows = PartitionRows::DensePages {
                        first,
                        stride,
                        words,
                        count: ids.len(),
                    };
                    continue;
                }
            }
            if bitmap_bytes < sparse_bytes {
                self.charge.try_grow_recoverable(bitmap_bytes)?;
                let mut pages = BTreeMap::new();
                let mut start = 0;
                while start < ids.len() {
                    let end =
                        start + ids[start..].partition_point(|r| r.page_id == ids[start].page_id);
                    let count = ids[end - 1].slot_id as usize / 64 + 1;
                    // Allocate once so actual capacity matches the representation
                    // estimate instead of Vec's geometric/minimum growth policy.
                    let mut words = Vec::with_capacity(count);
                    words.resize(count, 0);
                    for id in &ids[start..end] {
                        words[id.slot_id as usize / 64] |= 1u64 << (id.slot_id % 64);
                    }
                    pages.insert(ids[start].page_id, words);
                    start = end;
                }
                *rows = PartitionRows::Bitmap(pages);
            }
        }
        let retained = self
            .partitions
            .values()
            .map(|rows| match rows {
                PartitionRows::Sparse(ids) => 128 + ids.capacity() * std::mem::size_of::<RowId>(),
                PartitionRows::DensePages { words, .. } => 128 + words.capacity() * 8,
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
            Some(PartitionRows::DensePages {
                first,
                stride,
                words,
                ..
            }) => {
                let word = row.slot_id as usize / 64;
                if word >= *stride {
                    return false;
                }
                row.page_id
                    .checked_sub(*first)
                    .and_then(|page| {
                        words.get((page as usize).checked_mul(*stride)?.checked_add(word)?)
                    })
                    .is_some_and(|bits| bits & (1u64 << (row.slot_id % 64)) != 0)
            }
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
            Some(PartitionRows::DensePages { count, .. }) => *count,
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
                PartitionRows::DensePages {
                    first,
                    stride,
                    words,
                    ..
                } => {
                    visit_bitmap_words(
                        sid,
                        words.iter().enumerate().map(|(i, &word)| {
                            (*first + (i / stride) as u32, (i % stride) * 64, word)
                        }),
                        &mut f,
                    )?;
                }
                PartitionRows::Bitmap(pages) => {
                    visit_bitmap_words(
                        sid,
                        pages.iter().flat_map(|(&page, words)| {
                            words
                                .iter()
                                .enumerate()
                                .map(move |(i, &word)| (page, i * 64, word))
                        }),
                        &mut f,
                    )?;
                }
            }
        }
        Ok(())
    }
}

fn visit_bitmap_words(
    sid: u64,
    words: impl Iterator<Item = (u32, usize, u64)>,
    f: &mut impl FnMut(u64, &[RowId]) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    let _scratch = VectorQueryCharge::try_new_recoverable(256 * std::mem::size_of::<RowId>())?;
    let mut ids = Vec::with_capacity(256);
    for (page, start, mut bits) in words {
        while bits != 0 {
            let slot = start + bits.trailing_zeros() as usize;
            ids.push(RowId::new(page, slot as u16));
            bits &= bits - 1;
            if ids.len() == 256 {
                f(sid, &ids)?;
                ids.clear();
            }
        }
    }
    if !ids.is_empty() {
        f(sid, &ids)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filtered_vector_selection_uses_actual_bitmap_capacity() {
        let mut rows = RowSelection::new().unwrap();
        let ids: Vec<_> = (0..32).map(|i| RowId::new(4, i * 41)).collect();
        rows.append(7, &ids).unwrap();
        rows.seal().unwrap();
        assert!(matches!(rows.partitions[&7], PartitionRows::Bitmap(_)));
        assert!(rows.charge.bytes() <= 128 + ids.len() * std::mem::size_of::<RowId>());
        assert_eq!(rows.len(), ids.len());
        assert!(ids.iter().all(|&id| rows.contains(7, id)));
    }
    #[test]
    fn filtered_vector_dense_pages_preserve_holes_and_slot_bounds() {
        let mut rows = RowSelection::new().unwrap();
        let mut ids: Vec<_> = (10..140)
            .filter(|&page| page != 17)
            .flat_map(|page| [RowId::new(page, 1), RowId::new(page, 71)])
            .collect();
        rows.append(7, &ids).unwrap();
        rows.append(7, &ids[..25]).unwrap();
        rows.seal().unwrap();
        assert!(matches!(
            rows.partitions[&7],
            PartitionRows::DensePages { .. }
        ));
        assert_eq!(rows.partition_len(7), ids.len());
        for absent in [
            RowId::new(9, 1),
            RowId::new(17, 1),
            RowId::new(10, 129),
            RowId::new(11, 7),
            RowId::new(u32::MAX, 1),
        ] {
            assert!(!rows.contains(7, absent), "{absent:?}");
        }
        let mut visited = Vec::new();
        rows.visit(|sid, batch| {
            assert_eq!(sid, 7);
            visited.extend_from_slice(batch);
            Ok(())
        })
        .unwrap();
        ids.sort_unstable();
        assert_eq!(visited, ids);
        assert!(ids.iter().all(|&id| rows.contains(7, id)));
    }
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
