//! Residual-free existence joins retain keys, never build rows or hit lists.
use super::*;
use crate::engine::executor::{mem_budget, work_area};

type PairSet = hashbrown::HashSet<u64, ahash::RandomState>;

pub(super) struct PairExistenceState {
    keys: PairSet,
    _charge: mem_budget::ScopedCharge,
    _work_area: Option<work_area::WorkAreaHandle>,
}

#[inline]
fn pack(first: i64, second: i64) -> Option<u64> {
    let first = i32::try_from(first).ok()? as u32;
    let second = i32::try_from(second).ok()? as u32;
    Some((u64::from(first) << 32) | u64::from(second))
}

fn table_bytes(capacity: usize) -> usize {
    if capacity == 0 {
        0
    } else {
        capacity
            .next_power_of_two()
            .saturating_mul(9)
            .saturating_add(16)
    }
}

fn requested_bytes(rows: usize) -> Option<usize> {
    let buckets = rows
        .checked_mul(8)?
        .div_ceil(7)
        .max(4)
        .checked_next_power_of_two()?;
    buckets.checked_mul(9)?.checked_add(16)
}

/// Reserve old/new table overlap before growth. A refusal leaves every
/// consumed key replayable and never converts a recoverable miss into 1037.
fn reserve(
    keys: &mut PairSet,
    rows: usize,
    charge: &mut mem_budget::ScopedCharge,
    area: Option<&work_area::WorkAreaHandle>,
    cap: usize,
) -> bool {
    if rows <= keys.capacity() {
        return true;
    }
    let Some(bytes) = requested_bytes(rows) else {
        return false;
    };
    if !charge.add_within(bytes, cap) {
        return false;
    }
    if area.is_some_and(|area| {
        matches!(area.try_charge(bytes), work_area::SpillHint::Spill)
            && matches!(
                area.try_borrow_for_pipeline_drain(),
                work_area::SpillHint::Spill
            )
    }) {
        charge.release_bytes(bytes);
        area.expect("area checked").release(bytes);
        return false;
    }
    let old = table_bytes(keys.capacity());
    if keys.try_reserve(rows - keys.len()).is_err() {
        charge.release_bytes(bytes);
        if let Some(area) = area {
            area.release(bytes);
        }
        return false;
    }
    let actual = table_bytes(keys.capacity());
    let released = old.saturating_add(bytes).saturating_sub(actual);
    charge.release_bytes(released);
    if let Some(area) = area {
        area.release(released);
    }
    true
}

/// Reconstruct only the consumed distinct keys on a late type/domain or
/// budget refusal. SEMI/ANTI have no observable build multiplicity/payload.
struct Replay<'a> {
    keys: Option<hashbrown::hash_set::IntoIter<u64>>,
    charge: Option<mem_budget::ScopedCharge>,
    tail: Option<Batch>,
    remainder: Box<dyn BatchOperator + 'a>,
    columns: [usize; 2],
}

impl BatchOperator for Replay<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        self.remainder.schema()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if let Some(keys) = self.keys.as_mut().filter(|keys| keys.len() != 0) {
            let rows = keys.len().min(DEFAULT_BATCH_ROWS);
            let mut charge = mem_budget::ScopedCharge::new(0);
            charge.add_force_recoverable(
                rows.saturating_mul(16).saturating_add(
                    self.remainder
                        .schema()
                        .len()
                        .saturating_mul(std::mem::size_of::<ColumnVector>())
                        + 128,
                ),
            )?;
            let mut first = Vec::with_capacity(rows);
            let mut second = Vec::with_capacity(rows);
            for key in keys.take(rows) {
                first.push((key >> 32) as u32 as i32 as i64);
                second.push(key as u32 as i32 as i64);
            }
            let mut columns = vec![ColumnVector::Null { len: rows }; self.remainder.schema().len()];
            columns[self.columns[0]] = ColumnVector::I64 {
                data: first,
                nulls: None,
            };
            columns[self.columns[1]] = ColumnVector::I64 {
                data: second,
                nulls: None,
            };
            return Ok(Some(
                Batch::new(self.remainder.schema().clone(), columns).with_memory_charge(charge)?,
            ));
        }
        self.keys = None;
        self.charge = None;
        match self.tail.take() {
            Some(batch) => Ok(Some(batch)),
            None => self.remainder.next_batch(),
        }
    }
}

fn unique_build_hint(plan: &PhysicalPlan, schema: &BatchSchema, columns: &[usize]) -> usize {
    let table = match plan {
        PhysicalPlan::SeqScan { table, .. }
        | PhysicalPlan::ParallelSeqScan { table, .. }
        | PhysicalPlan::ZoneMapScan { table, .. }
        | PhysicalPlan::ParallelZoneMapScan { table, .. }
        | PhysicalPlan::IndexFastScan { table, .. }
        | PhysicalPlan::ParallelIndexFastScan { table, .. }
        | PhysicalPlan::IndexRangeScan { table, .. }
        | PhysicalPlan::ParallelIndexRangeScan { table, .. }
        | PhysicalPlan::IndexLookup { table, .. }
        | PhysicalPlan::IndexMultiLookup { table, .. }
        | PhysicalPlan::IndexInLookup { table, .. } => table,
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Exchange { input, .. } => return unique_build_hint(input, schema, columns),
        _ => return 0,
    };
    let unique = table.schema.iter_queryable_indexes().any(|index| {
        index.unique
            && !index.columns.is_empty()
            && !index.has_prefix_columns()
            && index.columns.iter().all(|name| {
                columns
                    .iter()
                    .any(|&column| schema.fields()[column].name.eq_ignore_ascii_case(name))
            })
    });
    if unique {
        crate::planner::estimated_rows(plan).max(0.0) as usize
    } else {
        0
    }
}

impl<'a> HashJoinBatchOperator<'a> {
    pub(super) fn try_initialize_pair_existence(&mut self) -> Result<bool, QueryError> {
        use crate::parser::JoinType;
        if !matches!(self.join_type, JoinType::Semi | JoinType::Anti)
            || self.residual.is_some()
            || self.build_is_left
            || self.build_keys.len() != 2
            || self.probe_keys.len() != 2
            || self.build_keys[0] == self.build_keys[1]
            || self.null_safe.iter().any(|value| *value)
            || self.key_collations.iter().any(Option::is_some)
            || !self.build_keys.iter().all(|&column| {
                vec_hash_join::schema_column_uses_i64_lane(self.build.schema(), column)
            })
            || !self.probe_keys.iter().all(|&column| {
                vec_hash_join::schema_column_uses_i64_lane(self.probe.schema(), column)
            })
            || !crate::util::env::flag("MYDB_NATIVE_PAIR_EXISTENCE", true)
        {
            return Ok(false);
        }
        let limits = crate::engine::executor::select::plan::hash_join_limits();
        let area = register_hash_join_work_area();
        let budget = mem_budget::current_budget();
        let mut cap = budget
            .as_ref()
            .map_or(limits.mem_limit_bytes as usize, |budget| budget.limit());
        if limits.mem_limit_explicit || area.is_none() {
            cap = cap.min(limits.mem_limit_bytes as usize);
        }
        if area.is_none() {
            cap = cap.min(
                budget
                    .as_ref()
                    .map_or(usize::MAX, |budget| budget.remaining()),
            );
        }
        let hint = self.build_plan.map_or(0, |plan| {
            unique_build_hint(plan, self.build.schema(), &self.build_keys)
        });
        let hint = if limits.max_build_rows == 0 {
            hint
        } else {
            hint.min(limits.max_build_rows as usize)
        };
        let hint = if requested_bytes(hint).is_some_and(|bytes| bytes <= cap) {
            hint
        } else {
            0
        };
        let mut keys = PairSet::default();
        let mut charge = mem_budget::ScopedCharge::new(0);
        let mut rows = 0u64;
        let columns = [self.build_keys[0], self.build_keys[1]];
        while let Some(batch) = self.build.next_batch()? {
            crate::engine::cancel::check_cancelled()?;
            let visible = batch.visible_indices().collect::<Vec<_>>();
            for (position, &row) in visible.iter().enumerate() {
                rows = rows.saturating_add(1);
                let first = &batch.columns()[columns[0]];
                let second = &batch.columns()[columns[1]];
                if first.is_null(row) || second.is_null(row) {
                    continue;
                }
                let key = first
                    .as_i64_slice()
                    .zip(second.as_i64_slice())
                    .and_then(|(first, second)| pack(first[row], second[row]));
                let accepted = key.is_some_and(|key| {
                    if keys.contains(&key) {
                        return true;
                    }
                    if limits.max_build_rows != 0 && rows > limits.max_build_rows {
                        return false;
                    }
                    let desired = (keys.len() + 1).max(if keys.is_empty() {
                        hint.min(usize::MAX - 1)
                    } else {
                        0
                    });
                    if !reserve(&mut keys, desired, &mut charge, area.as_ref(), cap) {
                        return false;
                    }
                    keys.insert(key);
                    true
                });
                if !accepted {
                    let tail = batch.with_selection(Selection::Indices(
                        visible[position..].iter().map(|row| *row as u32).collect(),
                    ));
                    let build_schema = self.build.schema().clone();
                    let remainder = std::mem::replace(
                        &mut self.build,
                        Box::new(MaterializedBatchOperator::new(build_schema, Vec::new())),
                    );
                    self.build = Box::new(Replay {
                        keys: Some(keys.into_iter()),
                        charge: Some(charge),
                        tail: Some(tail),
                        remainder,
                        columns,
                    });
                    return Ok(false);
                }
            }
        }
        analyze_stats::add_pending_join_runtime(rows, 0, 0);
        self.build = Box::new(MaterializedBatchOperator::new(
            self.build.schema().clone(),
            Vec::new(),
        ));
        if keys.is_empty() {
            if self.join_type == JoinType::Anti {
                self.empty_build_probe_passthrough = true;
            } else {
                self.probe = Box::new(MaterializedBatchOperator::new(
                    self.probe.schema().clone(),
                    Vec::new(),
                ));
                self.finished = true;
            }
        } else {
            self.pair_existence = Some(PairExistenceState {
                keys,
                _charge: charge,
                _work_area: area,
            });
        }
        Ok(true)
    }

    pub(super) fn next_pair_existence_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        loop {
            crate::engine::cancel::check_cancelled()?;
            let Some(batch) = self.probe.next_batch()? else {
                self.finish_input();
                return Ok(None);
            };
            let keys = &self
                .pair_existence
                .as_ref()
                .expect("existence build initialized")
                .keys;
            let mut selected = Vec::with_capacity(batch.visible_rows());
            let mut matched = 0u64;
            let mut scratch = Vec::with_capacity(17);
            for row in batch.visible_indices() {
                let mut pair = [0i64; 2];
                let usable = self.probe_keys.iter().enumerate().all(|(slot, &column)| {
                    let column = &batch.columns()[column];
                    if column.is_null(row) { return false; }
                    let value = if let Some(values) = column.as_i64_slice() { Some(values[row]) }
                    else {
                        scratch.clear();
                        crate::engine::executor::vec::key_encode::encode_nonnull_from_column_row_with_collation(column, row, None, &mut scratch);
                        crate::engine::executor::vec::key_encode::single_integer_key(&scratch).and_then(|value| i64::try_from(value).ok())
                    };
                    if let Some(value) = value { pair[slot] = value; true } else { false }
                });
                let exists =
                    usable && pack(pair[0], pair[1]).is_some_and(|key| keys.contains(&key));
                matched += u64::from(exists);
                if exists == (self.join_type == crate::parser::JoinType::Semi) {
                    selected.push(row as u32);
                }
            }
            analyze_stats::add_pending_join_runtime(0, batch.visible_rows() as u64, matched);
            self.report_pushed_runtime_rejections();
            if !selected.is_empty() {
                return Ok(Some(batch.with_selection(Selection::Indices(selected))));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_pair_domain_is_lossless_and_reservations_refuse_without_mutation() {
        for first in [i32::MIN as i64, -1, 0, i32::MAX as i64] {
            for second in [i32::MIN as i64, -1, 0, i32::MAX as i64] {
                let key = pack(first, second).unwrap();
                assert_eq!(
                    ((key >> 32) as u32 as i32 as i64, key as u32 as i32 as i64),
                    (first, second)
                );
            }
        }
        assert!(pack(i32::MIN as i64 - 1, 0).is_none());
        assert!(pack(0, i32::MAX as i64 + 1).is_none());
        let budget = Arc::new(mem_budget::MemBudget::new(512));
        mem_budget::with_query_budget(budget.clone(), || {
            let mut set = PairSet::default();
            let mut charge = mem_budget::ScopedCharge::new(0);
            assert!(reserve(&mut set, 1, &mut charge, None, 512));
            set.insert(pack(-1, 9).unwrap());
            let before = (set.capacity(), charge.charged());
            assert!(!reserve(&mut set, 4096, &mut charge, None, 512));
            assert_eq!((set.capacity(), charge.charged()), before);
            assert!(set.contains(&pack(-1, 9).unwrap()));
        });
        assert_eq!(budget.used(), 0);
    }
}
