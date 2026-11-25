//! Incremental recursive UNION membership with a bounded resident index.

use super::{CteResultRowCursor, CteSpillBuilder};
use crate::engine::api::CteResult;
use crate::engine::executor::memory_grant::{GrantResize, OperatorMemoryGrant};
use crate::engine::expr::InlineKeyBuf;
use crate::types::{Column, ColumnType, QueryError, Value};
use crate::util::fast_hash::{ah_hash_set, AhHashSet};
use std::{path::PathBuf, sync::Arc};

mod integers;

enum ResidentKeys {
    Encoded(AhHashSet<InlineKeyBuf>),
    Integers(integers::IntegerKeys),
}

pub(super) struct SeenKeys {
    resident: Option<ResidentKeys>,
    heap_bytes: usize,
    probes: usize,
    grant: OperatorMemoryGrant,
    limit: usize,
    collations: Vec<Option<u16>>,
    data_dir: PathBuf,
    // Binary compaction bounds the number of live spool handles by log(rows).
    levels: Vec<Option<Arc<CteResult>>>,
}

impl SeenKeys {
    pub(super) fn new(columns: &[Column], data_dir: PathBuf) -> Self {
        let limit = super::cte_spill_threshold_bytes();
        Self {
            resident: (limit != 0).then(|| {
                if columns.len() == 1
                    && matches!(
                        columns[0].column_type,
                        ColumnType::Tiny
                            | ColumnType::Short
                            | ColumnType::MediumInt
                            | ColumnType::Long
                            | ColumnType::LongLong
                    )
                {
                    ResidentKeys::Integers(integers::IntegerKeys::new())
                } else {
                    ResidentKeys::Encoded(ah_hash_set())
                }
            }),
            heap_bytes: 0,
            probes: 0,
            grant: OperatorMemoryGrant::register(),
            limit,
            collations: columns.iter().map(|c| c.source.collation_id).collect(),
            data_dir,
            levels: Vec::new(),
        }
    }

    #[inline]
    pub(super) fn insert(&mut self, row: &[Value]) -> Result<bool, QueryError> {
        self.probes = self.probes.wrapping_add(1);
        if let Some(ResidentKeys::Integers(keys)) = &mut self.resident {
            if let Some(fresh) = keys.insert(row, &mut self.grant, self.limit, self.probes)? {
                return Ok(fresh);
            }
            self.spill_resident()?;
        }
        self.insert_encoded(row)
    }

    fn insert_encoded(&mut self, row: &[Value]) -> Result<bool, QueryError> {
        let key = crate::engine::executor::dispatch::set_row_key(&self.collations, row);
        let _key_charge = if key.spilled() {
            Some(crate::engine::executor::mem_budget::force_charge_scoped(
                key.capacity(),
            )?)
        } else {
            None
        };
        if let Some(ResidentKeys::Encoded(set)) = &self.resident {
            if set.contains(&key) {
                return Ok(false);
            }
            let heap = if key.spilled() { key.capacity() } else { 0 };
            let capacity = if set.len() == set.capacity() {
                set.capacity().saturating_mul(2).max(128)
            } else {
                set.capacity()
            };
            let bytes = crate::engine::executor::memory_footprint::hash_table_capacity_bytes::<
                InlineKeyBuf,
            >(capacity)
            .saturating_add(self.heap_bytes)
            .saturating_add(heap);
            let check = bytes > self.grant.charged_bytes() || self.probes & 255 == 0;
            let target = bytes
                .max(self.grant.charged_bytes())
                .max(64 * 1024)
                .min(self.limit);
            if bytes <= self.limit
                && (!check || matches!(self.grant.resize(target), GrantResize::Granted))
            {
                let Some(ResidentKeys::Encoded(set)) = self.resident.as_mut() else {
                    unreachable!()
                };
                if set.len() == set.capacity() {
                    set.try_reserve(capacity.saturating_sub(set.len()))
                        .map_err(|_| {
                            crate::engine::executor::mem_budget::allocation_error(
                                "recursive UNION keys",
                            )
                        })?;
                }
                self.heap_bytes += heap;
                set.insert(key);
                return Ok(true);
            }
            self.spill_resident()?;
        }
        for segment in self.levels.iter().flatten() {
            let mut rows = CteResultRowCursor::open(segment.clone())?;
            while let Some(row) = rows.next_row()? {
                if matches!(&row[0], Value::Bytes(bytes) if bytes.as_slice() == key.as_slice()) {
                    return Ok(false);
                }
            }
        }
        let mut writer = self.writer()?;
        writer.push(vec![Value::Bytes(key.into_vec())])?;
        self.add_segment(Arc::new(writer.finish()?))?;
        Ok(true)
    }

    fn spill_resident(&mut self) -> Result<(), QueryError> {
        // Convert representation only: SQL and assignment never replay.
        let mut writer = self.writer()?;
        match self.resident.take().expect("resident recursive keys") {
            ResidentKeys::Encoded(keys) => {
                for key in keys {
                    writer.push(vec![Value::Bytes(key.into_vec())])?;
                }
            }
            ResidentKeys::Integers(keys) => keys.drain(|value| {
                let key =
                    crate::engine::executor::dispatch::set_row_key(&self.collations, &[value]);
                writer.push(vec![Value::Bytes(key.into_vec())])
            })?,
        }
        let segment = Arc::new(writer.finish()?);
        self.grant.resize_after_spill(0);
        self.heap_bytes = 0;
        self.add_segment(segment)
    }

    fn writer(&self) -> Result<CteSpillBuilder, QueryError> {
        let mut builder = CteSpillBuilder::new(
            vec![Column {
                name: "key".into(),
                column_type: ColumnType::Blob,
                ..Default::default()
            }],
            self.data_dir.clone(),
        );
        builder.start_spill()?;
        Ok(builder)
    }

    fn add_segment(&mut self, mut segment: Arc<CteResult>) -> Result<(), QueryError> {
        loop {
            let rows = segment.estimated_rows.max(1.0) as usize;
            let level = (usize::BITS - 1 - rows.leading_zeros()) as usize;
            if self.levels.len() <= level {
                self.levels.resize_with(level + 1, || None);
            }
            let Some(prior) = self.levels[level].take() else {
                self.levels[level] = Some(segment);
                return Ok(());
            };
            let mut writer = self.writer()?;
            for input in [prior, segment] {
                let mut cursor = CteResultRowCursor::open(input)?;
                while let Some(row) = cursor.next_row()? {
                    writer.push(row)?;
                }
            }
            segment = Arc::new(writer.finish()?);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_segments_match_canonical_keys_across_boundaries_and_representation_change() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let columns = vec![Column {
            name: "n".into(),
            column_type: ColumnType::LongLong,
            ..Default::default()
        }];
        let mut seen = SeenKeys::new(&columns, dir.path().to_path_buf());
        let mut oracle = ah_hash_set();
        let mut values = vec![Value::Null];
        for n in [
            i64::MIN,
            i64::MIN + 255,
            i64::MIN + 256,
            -257,
            -256,
            -255,
            -1,
            0,
            1,
            255,
            256,
            257,
            65536,
            i64::MAX,
        ] {
            values.push(Value::Int(n));
        }
        values.extend([
            Value::UInt(i64::MAX as u64),
            Value::UInt(i64::MAX as u64 + 1),
            Value::UInt(u64::MAX),
        ]);
        values.extend((0..2000).map(|i| Value::Int(((i * 8191) % 5003) - 2500)));
        for value in values
            .iter()
            .chain(values.iter().rev())
            .chain(values.iter())
        {
            let row = [value.clone()];
            assert_eq!(
                seen.insert(&row).unwrap(),
                oracle.insert(crate::engine::executor::dispatch::set_row_key(
                    &[None],
                    &row
                )),
                "{value:?}"
            );
        }
        assert!(matches!(seen.resident, Some(ResidentKeys::Integers(_))));
        // A physically different exact value must switch representation
        // without changing equality or discarding the dirty cached segment.
        assert!(!seen.insert(&[Value::DecimalCompact(10, 1)]).unwrap());
        assert!(seen.resident.is_none());
        for value in values.iter().take(18) {
            assert!(!seen.insert(&[value.clone()]).unwrap());
        }
    }

    #[test]
    fn integer_segments_spill_before_growth_and_preserve_null_and_unsigned_keys() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            8 * 1024 * 1024,
        ));
        crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
            let columns = vec![Column {
                name: "n".into(),
                column_type: ColumnType::LongLong,
                ..Default::default()
            }];
            let mut seen = SeenKeys::new(&columns, dir.path().to_path_buf());
            seen.limit = 512;
            for n in [1, 2, 255] {
                assert!(seen.insert(&[Value::Int(n)]).unwrap());
            }
            assert!(seen.insert(&[Value::Null]).unwrap());
            assert!(seen.resident.is_some());
            assert!(seen.insert(&[Value::Int(256)]).unwrap());
            assert!(seen.resident.is_none());
            assert_eq!(seen.grant.charged_bytes(), 0);
            assert!(seen.insert(&[Value::UInt(u64::MAX)]).unwrap());
            for value in [
                Value::Int(1),
                Value::Int(2),
                Value::Int(255),
                Value::Int(256),
                Value::Null,
                Value::UInt(u64::MAX),
            ] {
                assert!(!seen.insert(&[value]).unwrap());
            }
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn incremental_keys_keep_exact_membership_after_resident_eviction() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            8 * 1024 * 1024,
        ));
        crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
            let columns = vec![Column {
                name: "value".into(),
                column_type: ColumnType::VarString,
                source: crate::types::ColumnSource {
                    collation_id: Some(45),
                    ..Default::default()
                },
            }];
            let mut seen = SeenKeys::new(&columns, dir.path().to_path_buf());
            seen.limit = 64 * 1024;
            for i in 0..12 {
                let text = format!("{i}{}", "Abc".repeat(1000));
                assert!(seen.insert(&[Value::String(text.clone())]).unwrap());
                if i == 0 {
                    assert!(seen.resident.is_some());
                }
                assert!(!seen.insert(&[Value::String(text.to_lowercase())]).unwrap());
            }
            assert!(seen.resident.is_none());
            assert_eq!(seen.grant.charged_bytes(), 0);
            assert!(seen.levels.iter().flatten().count() <= 4);
            for i in (0..12).rev() {
                assert!(!seen
                    .insert(&[Value::String(format!("{i}{}", "ABC".repeat(1000)))])
                    .unwrap());
            }
            assert!(seen.insert(&[Value::Null]).unwrap());
            assert!(!seen.insert(&[Value::Null]).unwrap());
        });
        assert_eq!(budget.used(), 0);
    }
}
