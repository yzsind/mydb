//! Direct Join → Aggregate input and output, preserving dense group updates.
use super::*;

/// Owns the direct input's transient groups, scratch, and reservation.
#[derive(Default)]
pub(super) struct DirectJoinState {
    groups: DirectGroups,
    charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
    key_values: Vec<Value>,
}

#[derive(Default)]
enum DirectGroups {
    #[default]
    Empty,
    General(JoinAggregateGroups),
    Count(DirectJoinCountGroups),
}

impl DirectGroups {
    fn general(&self) -> Option<&JoinAggregateGroups> {
        match self {
            Self::General(groups) => Some(groups),
            _ => None,
        }
    }
    fn general_mut(&mut self) -> Option<&mut JoinAggregateGroups> {
        match self {
            Self::General(groups) => Some(groups),
            _ => None,
        }
    }
    fn count(&self) -> Option<&DirectJoinCountGroups> {
        match self {
            Self::Count(groups) => Some(groups),
            _ => None,
        }
    }
    fn count_mut(&mut self) -> Option<&mut DirectJoinCountGroups> {
        match self {
            Self::Count(groups) => Some(groups),
            _ => None,
        }
    }
    fn take_general(&mut self) -> Option<JoinAggregateGroups> {
        if !matches!(self, Self::General(_)) {
            return None;
        }
        match std::mem::take(self) {
            Self::General(groups) => Some(groups),
            _ => unreachable!(),
        }
    }
    fn take_count(&mut self) -> Option<DirectJoinCountGroups> {
        if !matches!(self, Self::Count(_)) {
            return None;
        }
        match std::mem::take(self) {
            Self::Count(groups) => Some(groups),
            _ => unreachable!(),
        }
    }
}

/// Dense transient state for a build-preserved Join→Aggregate pipeline.  The
/// hot probe loop indexes `states[group * slots + slot]` directly; canonical
/// radix/hash buckets are built once after the probe completes so spill and
/// finalize continue to use the ordinary HashAggregate implementation.
pub(super) struct JoinAggregateGroups {
    pub(super) by_key: hashbrown::HashMap<Vec<u8>, u32, ahash::RandomState>,
    pub(super) by_i64: hashbrown::HashMap<i64, u32, ahash::RandomState>,
    pub(super) key_values: Vec<Vec<Value>>,
    pub(super) states: crate::engine::executor::hash_aggregate::AggregateStateArena,
    /// Build-side groups have already reserved their complete row bound.
    /// Probe-side groups instead charge capacity incrementally and may spill.
    pub(super) precharged_group_limit: Option<usize>,
}

/// Compact state for the common unique-build-key outer-join count pipeline.
/// Group handles are build row ids, so neither a second hash table nor one
/// canonical aggregate state/value allocation per group is needed.
struct DirectJoinCountGroups {
    keys: Vec<i64>,
    counts: Vec<Vec<u64>>,
}

impl JoinAggregateGroups {
    pub(super) fn append_group(&mut self, values: Vec<Value>) -> Result<u32, QueryError> {
        let handle = u32::try_from(self.key_values.len()).map_err(|_| {
            crate::engine::executor::mem_budget::allocation_error(
                "direct join aggregate has too many groups",
            )
        })?;
        self.key_values.push(values);
        let state_group = self.states.add_group();
        if state_group != handle as usize {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "direct join aggregate group/state arena diverged",
            ));
        }
        Ok(handle)
    }
}

fn direct_group_capacity_bytes(capacity: usize, slots: usize) -> usize {
    // Account the capacity-bearing owners, not their current length. Both
    // lookup maps are reserved because a nullable typed key can use the byte
    // map for NULL while non-NULL rows use the i64 map. The fixed allowance
    // covers hash-table control bytes and the small encoded key allocation;
    // state payloads are represented by the shared canonical state arena.
    let per_group = std::mem::size_of::<Vec<Value>>()
        .saturating_add(std::mem::size_of::<AggState>().saturating_mul(slots))
        .saturating_add(std::mem::size_of::<(Vec<u8>, u32)>())
        .saturating_add(std::mem::size_of::<(i64, u32)>())
        .saturating_add(64);
    capacity.saturating_mul(per_group)
}

fn ensure_direct_group_capacity(
    groups: &mut JoinAggregateGroups,
    charge: &mut Option<crate::engine::executor::mem_budget::ScopedCharge>,
) -> Result<(), QueryError> {
    if groups.key_values.len() != groups.key_values.capacity() {
        return Ok(());
    }
    let extra = groups.precharged_group_limit.map_or_else(
        || groups.key_values.capacity().max(64),
        |limit| {
            groups
                .key_values
                .capacity()
                .max(64)
                .min(limit.saturating_sub(groups.key_values.capacity()))
        },
    );
    if extra == 0 {
        return Err(crate::error::catalog::ER_INTERNAL_ERROR
            .error("direct build aggregate exceeded its preflight group bound"));
    }
    if let Some(charge) = charge
        .as_mut()
        .filter(|_| groups.precharged_group_limit.is_none())
    {
        charge.add(direct_group_capacity_bytes(extra, groups.states.slots()))?;
    }
    groups.key_values.reserve_exact(extra);
    groups.states.reserve_groups(extra);
    groups.by_key.reserve(extra);
    groups.by_i64.reserve(extra);
    Ok(())
}

#[inline]
fn update_direct_state_from_column(
    state: &mut AggState,
    func: AggregateKind,
    column: &ColumnVector,
    row: usize,
) -> Result<(), QueryError> {
    if column.is_null(row) {
        return Ok(());
    }
    match column {
        ColumnVector::I64 { data, .. } => state.update_i64(func, data[row]),
        ColumnVector::U64 { data, .. } => state.update_u64(func, data[row]),
        ColumnVector::F64 { data, .. } => state.update_f64(func, data[row]),
        ColumnVector::Decimal32 { data, scale, .. } => {
            state.update_decimal_parts(func, i128::from(data[row]), *scale)
        }
        ColumnVector::Decimal64 { data, scale, .. } => {
            state.update_decimal_parts(func, i128::from(data[row]), *scale)
        }
        ColumnVector::Decimal128 { data, scale, .. } => {
            state.update_decimal_parts(func, data[row], *scale)
        }
        _ => state.update_ref(func, &column.value_at(row)),
    }
}

impl HashAggregateCore {
    pub(crate) fn direct_join_columns(&self) -> (&[usize], &[AggregateSpec]) {
        (
            self.aggregate_plan.group_columns(),
            self.aggregate_plan.aggregates(),
        )
    }

    /// Temporarily translate aggregate input indexes through a pure column
    /// projection so an adjacent join remains visible to the direct sink.
    /// The caller must restore the returned indexes after the delegated call;
    /// finalized output metadata continues to belong to the projected schema.
    pub(crate) fn remap_direct_input_columns(
        &mut self,
        projection: &[usize],
    ) -> Option<(Vec<usize>, Vec<usize>)> {
        let saved_groups = self.group_columns.clone();
        let saved_inputs = self
            .aggs
            .iter()
            .map(|spec| spec.input_column)
            .collect::<Vec<_>>();
        let mapped_groups = saved_groups
            .iter()
            .map(|&column| projection.get(column).copied())
            .collect::<Option<Vec<_>>>()?;
        let mapped_inputs = self
            .aggs
            .iter()
            .map(|spec| {
                if spec.func == AggregateKind::CountStar {
                    Some(spec.input_column)
                } else {
                    projection.get(spec.input_column).copied()
                }
            })
            .collect::<Option<Vec<_>>>()?;
        self.group_columns = mapped_groups;
        for (spec, input) in self.aggs.iter_mut().zip(mapped_inputs) {
            spec.input_column = input;
        }
        self.aggregate_plan = crate::engine::executor::hash_aggregate::AggregatePlan::new(
            self.group_columns.clone(),
            self.aggs.clone(),
        );
        self.group_key_program = None;
        Some((saved_groups, saved_inputs))
    }

    pub(crate) fn restore_direct_input_columns(&mut self, saved: (Vec<usize>, Vec<usize>)) {
        self.group_columns = saved.0;
        for (spec, input) in self.aggs.iter_mut().zip(saved.1) {
            spec.input_column = input;
        }
        self.aggregate_plan = crate::engine::executor::hash_aggregate::AggregatePlan::new(
            self.group_columns.clone(),
            self.aggs.clone(),
        );
        self.group_key_program = self.input_schema.as_ref().and_then(|schema| {
            crate::engine::executor::hash_aggregate::GroupKeyProgram::compile(
                &self.group_columns,
                schema,
            )
        });
    }

    pub(crate) fn direct_join_group_capacity_bytes(&self, capacity: usize) -> usize {
        direct_group_capacity_bytes(capacity, self.aggs.len())
    }

    pub(crate) fn direct_join_count_capacity_bytes(&self, capacity: usize) -> usize {
        capacity.saturating_mul(
            std::mem::size_of::<i64>()
                .saturating_add(std::mem::size_of::<u64>().saturating_mul(self.aggs.len())),
        )
    }

    pub(crate) fn direct_join_parallel_count_capacity_bytes(
        &self,
        capacity: usize,
        workers: usize,
    ) -> usize {
        let workers = workers.max(1);
        let keys = capacity.saturating_mul(std::mem::size_of::<i64>());
        let counts = capacity
            .saturating_mul(std::mem::size_of::<u64>())
            .saturating_mul(self.aggs.len())
            .saturating_mul(workers);
        let matched_words = capacity
            .div_ceil(u64::BITS as usize)
            .saturating_mul(std::mem::size_of::<u64>())
            .saturating_mul(workers);
        keys.saturating_add(counts).saturating_add(matched_words)
    }

    pub(crate) fn reserve_direct_join_capacity(&mut self, bytes: usize) -> bool {
        if crate::engine::executor::mem_budget::current_budget().is_none() {
            return true;
        }
        if self.direct_build_rollup {
            let budget =
                crate::engine::executor::mem_budget::current_budget().expect("budget present");
            if budget.tracks_process() {
                // The join payload and its consumer coexist. Bound this
                // resident state by one unchanged query work area and borrow
                // only their overlap from the governed process pool.
                let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
                if charge.add_within(bytes, budget.limit()) {
                    self.direct_join.charge = Some(charge);
                    return true;
                }
                return false;
            }
        }
        match crate::engine::executor::mem_budget::ScopedCharge::new_strict(
            bytes,
            "direct join aggregate dense states",
        ) {
            Ok(charge) => {
                self.direct_join.charge = Some(charge);
                true
            }
            Err(_) => false,
        }
    }

    pub(crate) fn prepare_direct_join_count_groups(
        &mut self,
        build_group_column: &ColumnVector,
    ) -> Result<(), QueryError> {
        if !self
            .aggs
            .iter()
            .all(|spec| matches!(spec.func, AggregateKind::Count | AggregateKind::CountStar))
        {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "compact direct join aggregate requires COUNT states",
            ));
        }
        let ColumnVector::I64 { data, nulls } = build_group_column else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "compact direct join aggregate requires an integer build key",
            ));
        };
        if nulls
            .as_ref()
            .is_some_and(|validity| validity.count_ones() != data.len())
        {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "compact direct join aggregate requires a non-NULL unique build key",
            ));
        }
        self.direct_join.groups = DirectGroups::Count(DirectJoinCountGroups {
            keys: data.clone(),
            counts: self.aggs.iter().map(|_| vec![0; data.len()]).collect(),
        });
        Ok(())
    }

    pub(crate) fn install_direct_join_count_groups(
        &mut self,
        build_group_column: &ColumnVector,
        counts: Vec<Vec<u64>>,
    ) -> Result<(), QueryError> {
        let ColumnVector::I64 { data, nulls } = build_group_column else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "compact direct join aggregate requires an integer build key",
            ));
        };
        if counts.len() != self.aggs.len()
            || counts.iter().any(|values| values.len() != data.len())
            || nulls
                .as_ref()
                .is_some_and(|validity| validity.count_ones() != data.len())
        {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "compact direct join aggregate count layout mismatch",
            ));
        }
        self.direct_join.groups = DirectGroups::Count(DirectJoinCountGroups {
            keys: data.clone(),
            counts,
        });
        Ok(())
    }

    /// Create/lookup aggregate groups from the HashJoin build payload once and
    /// return a dense build-row → group-state handle map. Probe matches can
    /// then update a group without gathering joined columns or hashing the
    /// GROUP BY key again. Duplicate build rows that share the same group key
    /// intentionally share one handle.
    pub(crate) fn prepare_direct_join_build_groups(
        &mut self,
        build_columns: &[ColumnVector],
        group_sources: &[(usize, usize)],
    ) -> Result<Vec<u32>, QueryError> {
        if group_sources.len() != self.group_columns.len() {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "direct join aggregate group layout mismatch",
            ));
        }
        let rows = build_columns.first().map(ColumnVector::len).unwrap_or(0);
        let mut handles = Vec::with_capacity(rows);
        let initial_groups = rows.min(64);
        let mut groups = JoinAggregateGroups {
            precharged_group_limit: Some(rows),
            by_key: hashbrown::HashMap::with_capacity_and_hasher(
                initial_groups.max(1),
                ahash::RandomState::new(),
            ),
            by_i64: hashbrown::HashMap::with_capacity_and_hasher(
                initial_groups.max(1),
                ahash::RandomState::new(),
            ),
            key_values: Vec::with_capacity(initial_groups),
            states: crate::engine::executor::hash_aggregate::AggregateStateArena::with_capacity(
                &self.aggs,
                initial_groups,
            ),
        };
        for row in 0..rows {
            if let [(_, build_column)] = group_sources {
                if let Some(ColumnVector::I64 { data, nulls }) = build_columns.get(*build_column) {
                    if !nulls.as_ref().is_some_and(|validity| !validity.get(row)) {
                        let key = data[row];
                        let handle = if let Some(&handle) = groups.by_i64.get(&key) {
                            handle
                        } else {
                            ensure_direct_group_capacity(
                                &mut groups,
                                &mut self.direct_join.charge,
                            )?;
                            let handle = groups.append_group(vec![Value::Int(key)])?;
                            groups.by_i64.insert(key, handle);
                            handle
                        };
                        handles.push(handle);
                        continue;
                    }
                }
            }
            self.row_key_buf.clear();
            self.direct_join.key_values.clear();
            for (&group_column, &(_, build_column)) in
                self.group_columns.iter().zip(group_sources.iter())
            {
                let column = build_columns.get(build_column).ok_or_else(|| {
                    crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "direct join aggregate build group column out of bounds",
                    )
                })?;
                let collation_id = self
                    .input_schema
                    .as_ref()
                    .and_then(|schema| schema.fields().get(group_column))
                    .and_then(|field| field.source.collation_id);
                crate::engine::executor::vec::key_encode::encode_from_column_row_with_collation(
                    column,
                    row,
                    collation_id,
                    &mut self.row_key_buf,
                );
                self.direct_join.key_values.push(column.value_at(row));
            }
            let handle = if let Some(&handle) = groups.by_key.get(self.row_key_buf.as_slice()) {
                handle
            } else {
                ensure_direct_group_capacity(&mut groups, &mut self.direct_join.charge)?;
                let key = self.row_key_buf.clone();
                let values = std::mem::take(&mut self.direct_join.key_values);
                let handle = groups.append_group(values)?;
                groups.by_key.insert(key, handle);
                handle
            };
            handles.push(handle);
        }
        self.direct_join.groups = DirectGroups::General(groups);
        Ok(handles)
    }

    pub(crate) fn prepare_direct_join_probe_groups(&mut self, capacity: usize) {
        self.direct_join.groups = DirectGroups::General(JoinAggregateGroups {
            precharged_group_limit: None,
            by_key: hashbrown::HashMap::with_capacity_and_hasher(
                capacity.max(1),
                ahash::RandomState::new(),
            ),
            by_i64: hashbrown::HashMap::with_capacity_and_hasher(
                capacity.max(1),
                ahash::RandomState::new(),
            ),
            key_values: Vec::with_capacity(capacity),
            states: crate::engine::executor::hash_aggregate::AggregateStateArena::with_capacity(
                &self.aggs, capacity,
            ),
        });
    }

    pub(crate) fn prepare_direct_join_lazy_build_groups(&mut self, rows: usize) {
        self.prepare_direct_join_probe_groups(rows.min(64));
        self.direct_join
            .groups
            .general_mut()
            .expect("build groups initialized")
            .precharged_group_limit = Some(rows);
    }

    /// Prepare the implicit SQL group used by an aggregate without GROUP BY.
    /// HashJoin can then feed match multiplicities directly into the canonical
    /// aggregate states without materialising one joined row per match.
    pub(crate) fn prepare_direct_join_single_group(&mut self) {
        debug_assert!(self.group_columns.is_empty());
        self.direct_join.groups = DirectGroups::General(JoinAggregateGroups {
            precharged_group_limit: Some(1),
            by_key: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
            by_i64: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
            key_values: vec![Vec::new()],
            states: {
                let mut states =
                    crate::engine::executor::hash_aggregate::AggregateStateArena::with_capacity(
                        &self.aggs, 1,
                    );
                states.add_group();
                states
            },
        });
    }

    pub(crate) fn direct_join_single_group_count_star_only(&self) -> bool {
        self.group_columns.is_empty()
            && !self.aggs.is_empty()
            && self
                .aggs
                .iter()
                .all(|spec| spec.func == AggregateKind::CountStar)
    }

    /// Seed global COUNT(*) slots from the join's exact match cardinality.
    /// The canonical Count state/finalizer remains the sole semantics owner,
    /// but the join no longer dispatches one state update per matched pair.
    pub(crate) fn update_direct_join_single_group_count_star(
        &mut self,
        mut matches: u64,
    ) -> Result<(), QueryError> {
        debug_assert!(self.direct_join_single_group_count_star_only());
        let groups = self.direct_join.groups.general_mut().ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "direct join aggregate global state is missing",
            )
        })?;
        while matches != 0 {
            let chunk = matches.min(usize::MAX as u64) as usize;
            for slot in 0..groups.states.slots() {
                groups
                    .states
                    .state_mut(0, slot)
                    .expect("direct global group owns every aggregate slot")
                    .update_count_star_repeated(chunk);
            }
            matches -= chunk as u64;
        }
        Ok(())
    }

    /// Feed all build matches for one probe row into the implicit global
    /// group. Probe-sourced values and COUNT(*) use repeated updates, while
    /// build-sourced values retain exact per-hit semantics.
    pub(crate) fn update_direct_join_single_group(
        &mut self,
        build_columns: &[ColumnVector],
        build_rows: &[u32],
        probe_batch: Option<(&Batch, usize)>,
        aggregate_sources: &[Option<DirectJoinColumn>],
    ) -> Result<(), QueryError> {
        if build_rows.is_empty() {
            return Ok(());
        }
        let groups = self.direct_join.groups.general_mut().ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "direct join aggregate global state is missing",
            )
        })?;
        let repetitions = build_rows.len();
        for (slot, spec) in self.aggs.iter().enumerate() {
            let state = groups.states.state_mut(0, slot).ok_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "direct join aggregate global state is truncated",
                )
            })?;
            match aggregate_sources.get(slot).copied().flatten() {
                None => {
                    debug_assert_eq!(spec.func, AggregateKind::CountStar);
                    state.update_count_star_repeated(repetitions);
                }
                Some(DirectJoinColumn::Build(column)) => {
                    let values = build_columns.get(column).ok_or_else(|| {
                        crate::storage::error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "direct join aggregate build input out of bounds",
                        )
                    })?;
                    if spec.func == AggregateKind::Count {
                        let count = build_rows
                            .iter()
                            .filter(|&&row| !values.is_null(row as usize))
                            .count();
                        state.update_count_star_repeated(count);
                    } else {
                        for &build_row in build_rows {
                            update_direct_state_from_column(
                                state,
                                spec.func,
                                values,
                                build_row as usize,
                            )?;
                        }
                    }
                }
                Some(DirectJoinColumn::Probe(column)) => {
                    let Some((probe, probe_row)) = probe_batch else {
                        state.update_repeated_ref(spec.func, &Value::Null, repetitions)?;
                        continue;
                    };
                    let values = probe.columns().get(column).ok_or_else(|| {
                        crate::storage::error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "direct join aggregate probe input out of bounds",
                        )
                    })?;
                    if spec.func == AggregateKind::Count {
                        if !values.is_null(probe_row) {
                            state.update_count_star_repeated(repetitions);
                        }
                    } else {
                        let value = values.value_at(probe_row);
                        state.update_repeated_ref(spec.func, &value, repetitions)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Probe-side handles are used only for the current probe row, so an
    /// exhausted arena can be checkpointed between rows without invalidating
    /// build-side handles. Persist canonical partials, never finalized AVG or
    /// DISTINCT values, and reuse the ordinary partition merge at the end.
    fn grow_direct_probe_groups(&mut self) -> Result<(), QueryError> {
        let groups = self.direct_join.groups.general_mut().expect("probe groups");
        if ensure_direct_group_capacity(groups, &mut self.direct_join.charge).is_ok() {
            return Ok(());
        }
        self.spill_direct_probe_groups()?;
        let bytes = direct_group_capacity_bytes(64, self.aggs.len());
        self.direct_join.charge = Some(crate::engine::executor::mem_budget::force_charge_scoped(
            bytes,
        )?);
        self.prepare_direct_join_probe_groups(64);
        Ok(())
    }

    fn spill_direct_probe_groups(&mut self) -> Result<(), QueryError> {
        let Some(mut groups) = self.direct_join.groups.take_general() else {
            return Ok(());
        };
        // A bounded conversion batch coexists with the source arena. Its
        // reservation must remain live until every temporary bucket drops.
        let _scratch = crate::engine::executor::mem_budget::force_charge_scoped(
            direct_group_capacity_bytes(1024, self.aggs.len()),
        )?;
        for (group, values) in groups.key_values.drain(..).enumerate() {
            let mut key = Vec::new();
            for (&column, value) in self.group_columns.iter().zip(&values) {
                let collation = self
                    .input_schema
                    .as_ref()
                    .and_then(|schema| schema.fields().get(column))
                    .and_then(|field| field.source.collation_id);
                crate::engine::executor::vec::key_encode::encode_with_collation(
                    value, collation, &mut key,
                );
            }
            let pidx =
                (crate::engine::executor::vec::key_encode::fxhash(&key) & PARTITION_MASK) as usize;
            let states = (0..self.aggs.len())
                .map(|slot| {
                    std::mem::take(groups.states.state_mut(group, slot).expect("group state"))
                })
                .collect();
            self.partitions[pidx]
                .buckets
                .push(GroupBucket::new(key, values, states));
            if group % 1024 == 1023 {
                for pidx in 0..NUM_PARTITIONS {
                    if !self.partitions[pidx].is_empty() {
                        self.do_spill_partition(pidx)?;
                    }
                }
            }
        }
        for pidx in 0..NUM_PARTITIONS {
            if !self.partitions[pidx].is_empty() {
                self.do_spill_partition(pidx)?;
            }
        }
        drop(groups);
        self.direct_join.charge = None;
        Ok(())
    }

    /// Resolve a probe-side GROUP BY key once for all build matches of the
    /// current probe row. The returned dense handle is then shared by every
    /// aggregate update for that join multiplicity.
    pub(crate) fn direct_join_probe_group(
        &mut self,
        batch: &Batch,
        row: usize,
        sources: &[(usize, usize)],
    ) -> Result<u32, QueryError> {
        self.direct_join_row_group(batch.columns(), row, sources, false)
    }

    /// Lazily create INNER build groups. Their cached handles live throughout
    /// probing, so capacity growth must never spill/reset the group arena.
    pub(crate) fn direct_join_build_group(
        &mut self,
        columns: &[ColumnVector],
        row: usize,
        sources: &[(usize, usize)],
    ) -> Result<u32, QueryError> {
        self.direct_join_row_group(columns, row, sources, true)
    }

    fn direct_join_row_group(
        &mut self,
        columns: &[ColumnVector],
        probe_row: usize,
        group_sources: &[(usize, usize)],
        stable_handles: bool,
    ) -> Result<u32, QueryError> {
        if group_sources.len() != self.group_columns.len() {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "direct join probe group layout mismatch",
            ));
        }
        if let [(_, probe_column)] = group_sources {
            if let Some(ColumnVector::I64 { data, nulls }) = columns.get(*probe_column) {
                if !nulls
                    .as_ref()
                    .is_some_and(|validity| !validity.get(probe_row))
                {
                    let key = data[probe_row];
                    let groups = self
                        .direct_join
                        .groups
                        .general_mut()
                        .expect("direct probe groups prepared");
                    if let Some(&handle) = groups.by_i64.get(&key) {
                        return Ok(handle);
                    }
                    if stable_handles {
                        ensure_direct_group_capacity(
                            self.direct_join.groups.general_mut().expect("build groups"),
                            &mut self.direct_join.charge,
                        )?;
                    } else {
                        self.grow_direct_probe_groups()?;
                    }
                    let groups = self.direct_join.groups.general_mut().expect("probe groups");
                    let handle = groups.append_group(vec![Value::Int(key)])?;
                    groups.by_i64.insert(key, handle);
                    return Ok(handle);
                }
            }
        }
        self.row_key_buf.clear();
        self.direct_join.key_values.clear();
        for (&group_column, &(_, probe_column)) in
            self.group_columns.iter().zip(group_sources.iter())
        {
            let column = columns.get(probe_column).ok_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "direct join probe group column out of bounds",
                )
            })?;
            let collation_id = self
                .input_schema
                .as_ref()
                .and_then(|schema| schema.fields().get(group_column))
                .and_then(|field| field.source.collation_id);
            crate::engine::executor::vec::key_encode::encode_from_column_row_with_collation(
                column,
                probe_row,
                collation_id,
                &mut self.row_key_buf,
            );
            self.direct_join.key_values.push(column.value_at(probe_row));
        }
        let groups = self
            .direct_join
            .groups
            .general_mut()
            .expect("direct probe groups prepared");
        if let Some(&handle) = groups.by_key.get(self.row_key_buf.as_slice()) {
            return Ok(handle);
        }
        if stable_handles {
            ensure_direct_group_capacity(
                self.direct_join.groups.general_mut().expect("build groups"),
                &mut self.direct_join.charge,
            )?;
        } else {
            self.grow_direct_probe_groups()?;
        }
        let groups = self.direct_join.groups.general_mut().expect("probe groups");
        let values = std::mem::take(&mut self.direct_join.key_values);
        let handle = groups.append_group(values)?;
        groups.by_key.insert(self.row_key_buf.clone(), handle);
        Ok(handle)
    }

    /// Feed one accepted join pair into a pre-resolved group state. Aggregate
    /// semantics remain entirely in the canonical vector state transition.
    pub(crate) fn update_direct_join_group(
        &mut self,
        handle: u32,
        build_columns: &[ColumnVector],
        build_row: usize,
        probe_batch: Option<(&Batch, usize)>,
        aggregate_sources: &[Option<DirectJoinColumn>],
    ) -> Result<(), QueryError> {
        if let Some(groups) = self.direct_join.groups.count_mut() {
            let group = handle as usize;
            if group >= groups.keys.len() {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_INTERNAL_ERROR,
                    "compact direct join aggregate group handle is stale",
                ));
            }
            for (slot, spec) in self.aggs.iter().enumerate() {
                let increment = match aggregate_sources.get(slot).copied().flatten() {
                    None => {
                        debug_assert_eq!(spec.func, AggregateKind::CountStar);
                        true
                    }
                    Some(DirectJoinColumn::Build(column)) => build_columns
                        .get(column)
                        .is_some_and(|values| !values.is_null(build_row)),
                    Some(DirectJoinColumn::Probe(column)) => {
                        probe_batch.is_some_and(|(probe, probe_row)| {
                            probe
                                .columns()
                                .get(column)
                                .is_some_and(|values| !values.is_null(probe_row))
                        })
                    }
                };
                if increment {
                    groups.counts[slot][group] = groups.counts[slot][group].saturating_add(1);
                }
            }
            return Ok(());
        }
        let groups = self.direct_join.groups.general_mut().ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "direct join aggregate group state is missing",
            )
        })?;
        for (slot, spec) in self.aggs.iter().enumerate() {
            let state = groups
                .states
                .state_mut(handle as usize, slot)
                .ok_or_else(|| {
                    crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "direct join aggregate group handle is stale",
                    )
                })?;
            match aggregate_sources.get(slot).copied().flatten() {
                None => {
                    debug_assert_eq!(spec.func, AggregateKind::CountStar);
                    state.update_count_star_repeated(1);
                }
                Some(DirectJoinColumn::Build(column)) => {
                    let values = build_columns.get(column).ok_or_else(|| {
                        crate::storage::error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "direct join aggregate build input out of bounds",
                        )
                    })?;
                    if spec.func == AggregateKind::Count {
                        if !values.is_null(build_row) {
                            state.update_count_star_repeated(1);
                        }
                    } else {
                        update_direct_state_from_column(state, spec.func, values, build_row)?;
                    }
                }
                Some(DirectJoinColumn::Probe(column)) => {
                    if let Some((probe, probe_row)) = probe_batch {
                        let values = probe.columns().get(column).ok_or_else(|| {
                            crate::storage::error(
                                crate::error::catalog::ER_UNKNOWN_ERROR,
                                "direct join aggregate probe input out of bounds",
                            )
                        })?;
                        if spec.func == AggregateKind::Count {
                            if !values.is_null(probe_row) {
                                state.update_count_star_repeated(1);
                            }
                        } else {
                            update_direct_state_from_column(state, spec.func, values, probe_row)?;
                        }
                    } else {
                        state.update_ref(spec.func, &Value::Null)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Apply the exact multiplicity of all build matches for one probe-side
    /// group without dispatching one aggregate transition per joined pair.
    /// Probe-sourced decomposable values are repeated directly in the
    /// canonical state; build-sourced values are folded over the matching
    /// build slice, preserving NULL and duplicate semantics.
    pub(crate) fn update_direct_join_probe_group_counts(
        &mut self,
        handle: u32,
        build_columns: &[ColumnVector],
        build_rows: &[u32],
        probe: &Batch,
        probe_row: usize,
        aggregate_sources: &[Option<DirectJoinColumn>],
    ) -> Result<(), QueryError> {
        if build_rows.is_empty() {
            return Ok(());
        }
        let groups = self.direct_join.groups.general_mut().ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "direct join aggregate group state is missing",
            )
        })?;
        for (slot, spec) in self.aggs.iter().enumerate() {
            if !matches!(
                spec.func,
                AggregateKind::Count
                    | AggregateKind::CountStar
                    | AggregateKind::Sum
                    | AggregateKind::Avg
            ) {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_INTERNAL_ERROR,
                    "multiplicity aggregate received a non-decomposable slot",
                ));
            }
            let state = groups
                .states
                .state_mut(handle as usize, slot)
                .ok_or_else(|| {
                    crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "direct join aggregate group handle is stale",
                    )
                })?;
            match aggregate_sources.get(slot).copied().flatten() {
                None => state.update_count_star_repeated(build_rows.len()),
                Some(DirectJoinColumn::Build(column)) => {
                    let values = build_columns.get(column).ok_or_else(|| {
                        crate::storage::error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "direct join aggregate build input out of bounds",
                        )
                    })?;
                    if spec.func == AggregateKind::Count {
                        let count = if values
                            .nulls()
                            .is_none_or(|validity| validity.count_ones() == values.len())
                        {
                            build_rows.len()
                        } else {
                            build_rows
                                .iter()
                                .filter(|&&row| !values.is_null(row as usize))
                                .count()
                        };
                        state.update_count_star_repeated(count);
                    } else {
                        for &row in build_rows {
                            update_direct_state_from_column(
                                state,
                                spec.func,
                                values,
                                row as usize,
                            )?;
                        }
                    }
                }
                Some(DirectJoinColumn::Probe(column)) => {
                    let values = probe.columns().get(column).ok_or_else(|| {
                        crate::storage::error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "direct join aggregate probe input out of bounds",
                        )
                    })?;
                    if spec.func == AggregateKind::Count {
                        if !values.is_null(probe_row) {
                            state.update_count_star_repeated(build_rows.len());
                        }
                    } else {
                        let value = values.value_at(probe_row);
                        state.update_repeated_ref(spec.func, &value, build_rows.len())?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn finish_direct_join_input(
        &mut self,
        batches: u64,
        rows: u64,
    ) -> Result<(), QueryError> {
        self.batches_consumed = self.batches_consumed.saturating_add(batches);
        self.native_rows_consumed = self.native_rows_consumed.saturating_add(rows);
        self.direct_join_input = true;
        if let Some(groups) = self.direct_join.groups.count() {
            self.peak_mem_bytes = self.peak_mem_bytes.max(
                groups
                    .keys
                    .capacity()
                    .saturating_mul(std::mem::size_of::<i64>())
                    .saturating_add(groups.counts.iter().fold(0usize, |bytes, counts| {
                        bytes.saturating_add(
                            counts.capacity().saturating_mul(std::mem::size_of::<u64>()),
                        )
                    })),
            );
        }
        if let Some(groups) = self.direct_join.groups.general() {
            let bytes = groups
                .key_values
                .capacity()
                .saturating_mul(std::mem::size_of::<Vec<Value>>())
                .saturating_add(
                    groups
                        .key_values
                        .iter()
                        .map(|values| {
                            values
                                .capacity()
                                .saturating_mul(std::mem::size_of::<Value>())
                        })
                        .sum::<usize>(),
                )
                .saturating_add(groups.states.allocated_bytes());
            self.peak_mem_bytes = self.peak_mem_bytes.max(bytes);
        }
        // Projection adapters restore column indexes after this callback.
        // Flush the tail while the source schema/collations still match the
        // direct grouping indexes used to write preceding partials.
        if self.direct_join.groups.general().is_some()
            && self.spilled_paths.iter().any(|paths| !paths.is_empty())
        {
            self.spill_direct_probe_groups()?;
        }
        Ok(())
    }

    /// Move canonical raw states to ROLLUP without finalizing AVG or cloning
    /// dimension values. The original reservation follows the moved payload.
    pub(crate) fn take_direct_rollup_groups(
        &mut self,
    ) -> Option<(
        Vec<GroupBucketSnapshot>,
        Option<crate::engine::executor::mem_budget::ScopedCharge>,
    )> {
        let mut groups = self.direct_join.groups.take_general()?;
        let buckets = groups
            .key_values
            .into_iter()
            .enumerate()
            .map(|(group, key_values)| {
                let aggs = (0..self.aggs.len())
                    .map(|slot| {
                        std::mem::take(
                            groups
                                .states
                                .state_mut(group, slot)
                                .expect("direct group state"),
                        )
                        .into_partial()
                    })
                    .collect();
                GroupBucketSnapshot {
                    key: Vec::new(),
                    key_values,
                    aggs,
                }
            })
            .collect();
        Some((buckets, self.direct_join.charge.take()))
    }

    pub(super) fn take_direct_join_output(
        &mut self,
    ) -> Result<Option<Box<dyn BatchOperator>>, QueryError> {
        if self.group_columns.is_empty()
            && self
                .direct_join
                .groups
                .general()
                .is_some_and(|groups| groups.key_values.is_empty())
            && self.spilled_paths.iter().all(|paths| paths.is_empty())
        {
            self.prepare_direct_join_single_group();
        }
        if self.direct_join.groups.general().is_some()
            && self.spilled_paths.iter().any(|paths| !paths.is_empty())
        {
            self.spill_direct_probe_groups()?;
        }
        if let Some(groups) = self.direct_join.groups.take_count() {
            let out_schema = self.output_schema();
            return Ok(Some(Box::new(DirectJoinCountOutputStream {
                schema: out_schema,
                keys: groups.keys,
                counts: groups.counts,
                cursor: 0,
                _charge: self.direct_join.charge.take(),
                _memory_grant: self.memory_grant.take(),
            })));
        }
        if let Some(groups) = self.direct_join.groups.take_general() {
            let out_schema = self.output_schema();
            return Ok(Some(Box::new(DirectJoinAggOutputStream {
                schema: out_schema,
                key_values: groups.key_values.into_iter(),
                slots: groups.states.slots(),
                states: groups.states,
                group_cursor: 0,
                _charge: self.direct_join.charge.take(),
                _memory_grant: self.memory_grant.take(),
            })));
        }
        Ok(None)
    }
}

struct DirectJoinAggOutputStream {
    schema: Arc<BatchSchema>,
    key_values: std::vec::IntoIter<Vec<Value>>,
    states: crate::engine::executor::hash_aggregate::AggregateStateArena,
    slots: usize,
    group_cursor: usize,
    _charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
    _memory_grant: Option<crate::engine::executor::memory_grant::OperatorMemoryGrant>,
}

struct DirectJoinCountOutputStream {
    schema: Arc<BatchSchema>,
    keys: Vec<i64>,
    counts: Vec<Vec<u64>>,
    cursor: usize,
    _charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
    _memory_grant: Option<crate::engine::executor::memory_grant::OperatorMemoryGrant>,
}

impl BatchOperator for DirectJoinCountOutputStream {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.cursor == self.keys.len() {
            return Ok(None);
        }
        let end = self
            .cursor
            .saturating_add(DEFAULT_BATCH_ROWS)
            .min(self.keys.len());
        let mut columns = Vec::with_capacity(1 + self.counts.len());
        columns.push(ColumnVector::I64 {
            data: self.keys[self.cursor..end].to_vec(),
            nulls: None,
        });
        columns.extend(self.counts.iter().map(|counts| {
            ColumnVector::I64 {
                data: counts[self.cursor..end]
                    .iter()
                    .map(|count| *count as i64)
                    .collect(),
                nulls: None,
            }
        }));
        self.cursor = end;
        Ok(Some(Batch::new(self.schema.clone(), columns)))
    }
}

impl BatchOperator for DirectJoinAggOutputStream {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let mut columns = self
            .schema
            .fields()
            .iter()
            .map(|field| ColumnAccumulator::for_field(field, DEFAULT_BATCH_ROWS))
            .collect::<Vec<_>>();
        let group_columns = self.schema.len().saturating_sub(self.slots);
        let mut rows = 0usize;
        while rows < DEFAULT_BATCH_ROWS {
            let Some(key_values) = self.key_values.next() else {
                break;
            };
            if key_values.len() != group_columns {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "direct join aggregate output group width mismatch",
                ));
            }
            for (column, value) in columns[..group_columns].iter_mut().zip(key_values.iter()) {
                column.push(value);
            }
            let partials = self
                .states
                .take_group_partials(self.group_cursor)
                .ok_or_else(|| {
                    crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "direct join aggregate output state is truncated",
                    )
                })?;
            for (slot, partial) in partials.into_iter().enumerate() {
                let value = partial.finalize(
                    self.states
                        .spec(slot)
                        .expect("direct output slot owns a specification")
                        .func,
                );
                columns[group_columns + slot].push(&value);
            }
            self.group_cursor += 1;
            rows += 1;
        }
        if rows == 0 {
            if self.group_cursor != self.states.groups() {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "direct join aggregate output has excess states",
                ));
            }
            return Ok(None);
        }
        Ok(Some(Batch::with_row_count(
            self.schema.clone(),
            columns.into_iter().map(ColumnAccumulator::finish).collect(),
            rows,
        )))
    }
}
