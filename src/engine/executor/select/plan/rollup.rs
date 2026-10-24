use super::*;

/// Bound retained row input by actual capacity, including stale/missing CBO
/// estimates. The prefix is transferred to the sorter without reopening input.
pub(in crate::engine::executor::select) enum PreparedRollupInput<'a> {
    Rows(
        Vec<PlanRow>,
        crate::engine::executor::mem_budget::ScopedCharge,
    ),
    Stream(Box<dyn PlanStream + 'a>),
}

pub(in crate::engine::executor::select) fn prepare_rollup_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<PreparedRollupInput<'a>, QueryError> {
    let input = match super::vec_bridge::try_native_plan_row_stream(db, txn, plan)? {
        Some(input) => input,
        None => execute_plan_stream(db, txn, plan)?,
    };
    prepare_rollup_stream(input)
}

fn prepare_rollup_stream<'a>(
    mut input: Box<dyn PlanStream + 'a>,
) -> Result<PreparedRollupInput<'a>, QueryError> {
    use crate::engine::executor::mem_budget::{current_budget, ScopedCharge};
    struct Prefix<'a> {
        rows: std::vec::IntoIter<PlanRow>,
        pending: Option<PlanRow>,
        input: Box<dyn PlanStream + 'a>,
        charge: Option<ScopedCharge>,
    }
    impl PlanStream for Prefix<'_> {
        fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
            if let Some(row) = self.rows.next() {
                return Ok(Some(row));
            }
            self.rows = Vec::new().into_iter();
            self.charge = None;
            if let Some(row) = self.pending.take() {
                return Ok(Some(row));
            }
            self.input.next()
        }
    }
    let cap = current_budget().map_or(8 * 1024 * 1024, |b| b.limit() / 4);
    let mut rows = Vec::new();
    let mut charge = ScopedCharge::new(0);
    while let Some(row) = input.next()? {
        crate::engine::cancel::check_cancelled()?;
        let bytes = std::mem::size_of::<PlanRow>()
            .saturating_mul(2)
            .saturating_add(super::vec_bridge::plan_row_owned_heap_bytes(&row));
        if charge.charged().saturating_add(bytes) > cap || charge.add(bytes).is_err() {
            return Ok(PreparedRollupInput::Stream(Box::new(Prefix {
                rows: rows.into_iter(),
                pending: Some(row),
                input,
                charge: Some(charge),
            })));
        }
        rows.push(row);
    }
    Ok(PreparedRollupInput::Rows(rows, charge))
}

pub(in crate::engine::executor::select) fn execute_hash_rollup_aggregate(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
) -> Result<Vec<PlanRow>, QueryError> {
    use super::aggregate::{collect_aggregate_slots, AggregateSlot};
    let mut slot_exprs: Vec<&Expr> = aggregates.iter().collect();
    if let Some(h) = having {
        slot_exprs.push(h);
    }
    let mut slots: Vec<AggregateSlot> = collect_aggregate_slots(&slot_exprs);
    super::aggregate::bind_slot_comparisons(&mut slots, input);

    // Compute the finest grouping set with the same columnar HashAggregate
    // used by ordinary GROUP BY, then seed the mergeable row accumulators for
    // the prefix derivation. Capability discovery completes before the input
    // is opened; once this returns `Some`, including for an empty input, this
    // pipeline owns execution and never restarts through the row path.
    if let Some(finest) =
        super::vec_bridge::vec_rollup_finest_groups(db, txn, input, group_by, &slots)?
    {
        return rollup_emit_from_vec_finest(finest, group_by, &slots, having);
    }
    if let Some(stream) =
        super::vec_bridge::try_native_rollup_sorted_stream(db, txn, input, group_by)?
    {
        return aggregate_rollup_plan_stream_sorted(stream, group_by, having, &slots);
    }
    let (input_rows, _input_charge) = match prepare_rollup_input(db, txn, input)? {
        PreparedRollupInput::Rows(rows, charge) => (rows, charge),
        PreparedRollupInput::Stream(stream) => {
            return aggregate_rollup_plan_stream_spilled(db, stream, group_by, having, &slots);
        }
    };
    // The spill decision is made INSIDE `aggregate_rollup_plan_rows`, driven by
    // the real accumulated group bytes (`add_within`), not by `input_rows.len()`.
    // The two-phase working set is bounded by the number of DISTINCT groups, not
    // the (already-materialized) input row count, so a wide fact-table join with
    // few groups (TPC-DS Q22: 2.4M rows → ~9k groups) stays in memory instead of
    // paying a needless multi-second external sort.
    aggregate_rollup_plan_rows(db, input_rows, group_by, having, &slots)
}

/// Seed the finest level from the vectorized grouping, then derive coarser
/// prefix levels and emit. The derive mirrors the in-memory phase 2 of
/// [`aggregate_rollup_plan_rows`] (truncate the key, `clone_box`/`merge_from`
/// up the prefix chain), minus the spill bail — the finest already fits in
/// memory and every coarser level is no larger than it. Emission reuses
/// [`emit_rollup_group_state`], so all the super-aggregate NULL-key /
/// GROUPING() / HAVING semantics stay on the proven row path.
pub(in crate::engine::executor::select) fn rollup_emit_from_vec_finest(
    mut finest: super::vec_bridge::VecRollupFinest,
    group_by: &[Expr],
    slots: &[super::aggregate::AggregateSlot],
    having: Option<&Expr>,
) -> Result<Vec<PlanRow>, QueryError> {
    use super::vec_bridge::{VecRollupAggState, VecRollupFinestGroup};
    use crate::engine::expr::ValueKey;
    let group_by_len = group_by.len();
    let mut out = RollupEmitTarget::new(group_by, group_by_len, slots);
    if finest.groups.is_empty() {
        let state = RollupGroupState {
            prefix_len: 0,
            key_values: Vec::new(),
            sample_fields: Vec::new(),
            accumulators: slots.iter().map(|slot| slot.make_accumulator()).collect(),
        };
        emit_rollup_group_state_to_target(&mut out, slots, group_by_len, state, having)?;
        return Ok(out.into_rows());
    }
    let field = crate::engine::executor::sort::SortField {
        ascending: true,
        nulls_first: true,
        collation_id: Some(crate::charset::default_collation_info().id),
    };
    let fields: Vec<_> = finest
        .collations
        .iter()
        .map(|&collation_id| crate::engine::executor::sort::SortField {
            collation_id,
            ..field
        })
        .collect();
    if !crate::engine::executor::sort::key_encode::try_sort_by_borrowed_encoded_keys(
        &mut finest.groups,
        &fields,
        |group| group.key_values.as_slice(),
    )? {
        finest.groups.sort_unstable_by(|left, right| {
            for ((left, right), field) in left.key_values.iter().zip(&right.key_values).zip(&fields)
            {
                let order =
                    crate::engine::executor::sort::compare_value_for_sort(left, right, *field);
                if order != std::cmp::Ordering::Equal {
                    return order;
                }
            }
            left.key_values.len().cmp(&right.key_values.len())
        });
    }
    // Keep the established addition order for approximate SUM/AVG. Exact
    // decimal/count states are associative; floating-point states are not.
    let cascade = !finest
        .groups
        .iter()
        .flat_map(|group| &group.agg_states)
        .any(|state| matches!(state.sum, super::aggregate::NumericAggState::Float(_)));
    let names = slots
        .iter()
        .map(|slot| slot.name.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let merge = |target: &mut [VecRollupAggState],
                 source: &[VecRollupAggState]|
     -> Result<(), QueryError> {
        for ((target, source), name) in target.iter_mut().zip(source).zip(&names) {
            match name.as_str() {
                "SUM" | "AVG" => {
                    super::aggregate::merge_numeric_state(&mut target.sum, source.sum.clone());
                    target.count = target.count.saturating_add(source.count);
                }
                "COUNT" => target.count = target.count.saturating_add(source.count),
                "MIN" | "MAX" => {
                    let (target, source) = if name == "MIN" {
                        (&mut target.min, &source.min)
                    } else {
                        (&mut target.max, &source.max)
                    };
                    if let Some(value) = source {
                        let replace = match target.as_ref() {
                            None => true,
                            Some(current) => {
                                let order = crate::engine::expr::compare_values(value, current)?;
                                if name == "MIN" {
                                    order == std::cmp::Ordering::Less
                                } else {
                                    order == std::cmp::Ordering::Greater
                                }
                            }
                        };
                        if replace {
                            *target = Some(value.clone());
                        }
                    }
                }
                _ => unreachable!("vector rollup requires mergeable slots"),
            }
        }
        Ok(())
    };
    let emit = |out: &mut RollupEmitTarget, group: VecRollupFinestGroup, prefix_len| {
        let accumulators = slots
            .iter()
            .zip(group.agg_states)
            .map(|(slot, state)| {
                super::aggregate::seed_rollup_accumulator_owned(
                    slot,
                    state.sum,
                    state.count,
                    state.min,
                    state.max,
                )
            })
            .collect();
        emit_rollup_group_state_to_target(
            out,
            slots,
            group_by_len,
            RollupGroupState {
                prefix_len,
                key_values: group.key_values,
                sample_fields: Vec::new(),
                accumulators,
            },
            having,
        )
    };
    // Sorted finest keys make each coarser prefix contiguous. Keep one live
    // state per prefix instead of constructing a second hash table for the
    // finest level and retaining every level's boxed accumulator maps.
    let mut live: Vec<Option<VecRollupFinestGroup>> = (0..group_by_len).map(|_| None).collect();
    let mut completed: Vec<Vec<VecRollupFinestGroup>> =
        (0..group_by_len).map(|_| Vec::new()).collect();
    let mut previous: Option<Vec<Value>> = None;
    // Feed only the immediate parent. When a prefix closes, merge its
    // completed subtotal into the next parent once. Updating every live
    // prefix for every finest group performs depth * cardinality merges.
    let fold_parent = |live: &mut [Option<VecRollupFinestGroup>],
                       group: &VecRollupFinestGroup,
                       prefix: usize|
     -> Result<(), QueryError> {
        if let Some(parent) = live[prefix].as_mut() {
            merge(&mut parent.agg_states, &group.agg_states)
        } else {
            live[prefix] = Some(VecRollupFinestGroup {
                key_values: group.key_values[..prefix].to_vec(),
                agg_states: group.agg_states.clone(),
            });
            Ok(())
        }
    };
    for group in finest.groups {
        crate::engine::cancel::check_cancelled()?;
        let common = previous.as_ref().map_or(0, |previous| {
            previous
                .iter()
                .zip(&group.key_values)
                .zip(&finest.collations)
                .take_while(|((a, b), collation)| {
                    a == b
                        || ValueKey::from_with_collation(a, **collation)
                            == ValueKey::from_with_collation(b, **collation)
                })
                .count()
        });
        for prefix in ((common + 1)..group_by_len).rev() {
            if let Some(state) = live[prefix].take() {
                if cascade {
                    fold_parent(&mut live, &state, prefix - 1)?;
                }
                completed[prefix].push(state);
            }
        }
        if cascade {
            fold_parent(&mut live, &group, group_by_len - 1)?;
        } else {
            for prefix in 0..group_by_len {
                fold_parent(&mut live, &group, prefix)?;
            }
        }
        if let Some(previous) = previous.as_mut() {
            for (old, new) in previous[common..]
                .iter_mut()
                .zip(&group.key_values[common..])
            {
                old.clone_from(new);
            }
        } else {
            previous = Some(group.key_values.clone());
        }
        emit(&mut out, group, group_by_len)?;
    }
    // Retain level-major, key-sorted output and the original representative
    // values, including real NULL keys versus subtotal NULL markers.
    for prefix in (0..group_by_len).rev() {
        if let Some(state) = live[prefix].take() {
            if cascade && prefix > 0 {
                fold_parent(&mut live, &state, prefix - 1)?;
            }
            completed[prefix].push(state);
        }
        for group in std::mem::take(&mut completed[prefix]) {
            emit(&mut out, group, prefix)?;
        }
    }
    Ok(out.into_rows())
}

pub(in crate::engine::executor::select) struct RollupGroupState {
    pub(in crate::engine::executor::select) prefix_len: usize,
    pub(in crate::engine::executor::select) key_values: Vec<Value>,
    pub(in crate::engine::executor::select) sample_fields: Vec<PlanField>,
    pub(in crate::engine::executor::select) accumulators:
        Vec<Box<dyn super::aggregate::Accumulator>>,
}

struct GroupingSetGroupState {
    set_indices: Vec<usize>,
    group_values: Vec<Value>,
    sample_fields: Vec<PlanField>,
    accumulators: Vec<Box<dyn super::aggregate::Accumulator>>,
}

fn rollup_mask(group_by_len: usize, prefix_len: usize) -> u64 {
    let mut mask = 0u64;
    for idx in prefix_len..group_by_len {
        mask |= 1u64 << idx;
    }
    mask
}

fn grouping_set_mask(group_by_len: usize, set_indices: &[usize]) -> u64 {
    let mut present = vec![false; group_by_len];
    for idx in set_indices {
        if *idx < group_by_len {
            present[*idx] = true;
        }
    }
    let mut mask = 0u64;
    for (idx, is_present) in present.into_iter().enumerate() {
        if !is_present {
            mask |= 1u64 << idx;
        }
    }
    mask
}

enum RollupTopKPayload {
    Row(PlanRow),
    Compact {
        prefix_len: usize,
        key_values: Vec<Value>,
        agg_values: Vec<Value>,
    },
}

struct RollupTopKCandidate {
    order_key: Vec<Value>,
    payload: RollupTopKPayload,
}

#[derive(Clone, Copy)]
enum CompactRollupKey {
    Group(usize),
    Aggregate(usize),
}

struct RollupTopKBucket {
    candidates: Vec<RollupTopKCandidate>,
    boundary: usize,
    next_prune: usize,
}

impl Default for RollupTopKBucket {
    fn default() -> Self {
        Self {
            candidates: Vec::new(),
            boundary: usize::MAX,
            next_prune: 0,
        }
    }
}

struct RollupTopKCollector {
    hint: RollupWindowTopKHint,
    partition_keys: Option<Vec<CompactRollupKey>>,
    order_keys: Option<Vec<CompactRollupKey>>,
    group_by_len: usize,
    slots: Vec<super::aggregate::AggregateSlot>,
    sort_fields: Vec<crate::engine::executor::sort::SortField>,
    partitions: hashbrown::HashMap<Vec<ValueKey>, RollupTopKBucket, ahash::RandomState>,
}

impl RollupTopKCollector {
    fn new(
        hint: RollupWindowTopKHint,
        group_by: &[Expr],
        group_by_len: usize,
        slots: &[super::aggregate::AggregateSlot],
    ) -> Self {
        let sort_fields = crate::engine::executor::sort::fields_from_order_by(&hint.order_by);
        let compile = |expr: &Expr| {
            let group = match expr {
                Expr::GroupKeyRef { index, .. } => Some(*index),
                Expr::Identifier(_) | Expr::CompoundIdentifier(_) => group_by
                    .iter()
                    .position(|group| expr_matches_group_expr(expr, group)),
                _ => None,
            };
            if let Some(group) = group {
                return Some(CompactRollupKey::Group(group));
            }
            if matches!(expr, Expr::Function { .. }) {
                let key = super::aggregate::aggregate_key(expr);
                return slots
                    .iter()
                    .position(|slot| slot.key == key)
                    .map(CompactRollupKey::Aggregate);
            }
            None
        };
        let partition_keys = hint.partition_by.iter().map(compile).collect();
        let order_keys = hint
            .order_by
            .iter()
            .map(|item| compile(&item.expr))
            .collect();
        Self {
            hint,
            partition_keys,
            order_keys,
            group_by_len,
            slots: slots.to_vec(),
            sort_fields,
            partitions: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
        }
    }

    fn push(&mut self, row: PlanRow) -> Result<(), QueryError> {
        let partition_key = self
            .hint
            .partition_by
            .iter()
            .map(|expr| eval_aggregate_expr(expr, &[], &row).map(|v| ValueKey::from(&v)))
            .collect::<Result<smallvec::SmallVec<[_; 2]>, _>>()?;
        let order_key = self
            .hint
            .order_by
            .iter()
            .map(|item| eval_aggregate_expr(&item.expr, &[], &row))
            .collect::<Result<Vec<_>, _>>()?;
        self.push_candidate(&partition_key, order_key, RollupTopKPayload::Row(row));
        Ok(())
    }

    fn push_state(
        &mut self,
        slots: &[super::aggregate::AggregateSlot],
        group_by_len: usize,
        state: RollupGroupState,
        having: Option<&Expr>,
    ) -> Result<(), QueryError> {
        if having.is_some() || !state.sample_fields.is_empty() {
            if let Some(row) = build_rollup_group_row(slots, group_by_len, state, having)? {
                self.push(row)?;
            }
            return Ok(());
        }
        let RollupGroupState {
            prefix_len,
            key_values,
            accumulators,
            ..
        } = state;
        let mut agg_values = Vec::with_capacity(slots.len());
        for acc in accumulators {
            agg_values.push(acc.finalize()?);
        }
        let Some(partition_key) =
            self.eval_compact_partition_key(prefix_len, &key_values, &agg_values)
        else {
            let row =
                build_compact_rollup_row(slots, group_by_len, prefix_len, key_values, agg_values);
            self.push(row)?;
            return Ok(());
        };
        let Some(order_key) = self.eval_compact_order_key(prefix_len, &key_values, &agg_values)
        else {
            let row =
                build_compact_rollup_row(slots, group_by_len, prefix_len, key_values, agg_values);
            self.push(row)?;
            return Ok(());
        };
        self.push_candidate(
            &partition_key,
            order_key,
            RollupTopKPayload::Compact {
                prefix_len,
                key_values,
                agg_values,
            },
        );
        Ok(())
    }

    fn push_candidate(
        &mut self,
        partition_key: &[ValueKey],
        order_key: Vec<Value>,
        payload: RollupTopKPayload,
    ) {
        let bucket = self.partitions.entry_ref(partition_key).or_default();
        if bucket.boundary != usize::MAX {
            let order = crate::engine::executor::sort::compare_sort_keys(
                &self.sort_fields,
                &order_key,
                &bucket.candidates[bucket.boundary].order_key,
            );
            if order == std::cmp::Ordering::Greater
                || (order == std::cmp::Ordering::Equal
                    && matches!(
                        self.hint.kind,
                        crate::engine::executor::dispatch::RankKind::RowNumber
                    ))
            {
                return;
            }
        }
        bucket
            .candidates
            .push(RollupTopKCandidate { order_key, payload });
        let prune_at = self.hint.k.saturating_mul(4).saturating_add(64).max(128);
        if bucket.candidates.len() > bucket.next_prune.max(prune_at) {
            bucket.boundary = Self::prune_bucket(
                &mut bucket.candidates,
                &self.sort_fields,
                self.hint.k,
                self.hint.kind,
            );
            // Required peer ties can exceed K. Geometric pruning avoids an
            // O(N²) re-sort when a large tie group cannot be discarded.
            bucket.next_prune = prune_at.max(bucket.candidates.len().saturating_mul(2));
        }
    }

    fn eval_compact_partition_key(
        &self,
        prefix_len: usize,
        key_values: &[Value],
        agg_values: &[Value],
    ) -> Option<smallvec::SmallVec<[ValueKey; 2]>> {
        self.partition_keys
            .as_ref()?
            .iter()
            .map(|key| {
                Self::eval_compact_key(*key, prefix_len, key_values, agg_values)
                    .map(|value| ValueKey::from(&value))
            })
            .collect()
    }

    fn eval_compact_order_key(
        &self,
        prefix_len: usize,
        key_values: &[Value],
        agg_values: &[Value],
    ) -> Option<Vec<Value>> {
        self.order_keys
            .as_ref()?
            .iter()
            .map(|key| Self::eval_compact_key(*key, prefix_len, key_values, agg_values))
            .collect()
    }

    fn eval_compact_key(
        key: CompactRollupKey,
        prefix_len: usize,
        keys: &[Value],
        aggregates: &[Value],
    ) -> Option<Value> {
        match key {
            CompactRollupKey::Group(index) => Some(if index < prefix_len {
                keys.get(index).cloned().unwrap_or(Value::Null)
            } else {
                Value::Null
            }),
            CompactRollupKey::Aggregate(index) => aggregates.get(index).cloned(),
        }
    }

    fn into_rows(mut self) -> Vec<PlanRow> {
        let mut out = Vec::new();
        for (_, mut bucket) in self.partitions.drain() {
            Self::prune_bucket(
                &mut bucket.candidates,
                &self.sort_fields,
                self.hint.k,
                self.hint.kind,
            );
            out.reserve(bucket.candidates.len());
            out.extend(
                bucket
                    .candidates
                    .into_iter()
                    .map(|candidate| match candidate.payload {
                        RollupTopKPayload::Row(row) => row,
                        RollupTopKPayload::Compact {
                            prefix_len,
                            key_values,
                            agg_values,
                        } => build_compact_rollup_row(
                            &self.slots,
                            self.group_by_len,
                            prefix_len,
                            key_values,
                            agg_values,
                        ),
                    }),
            );
        }
        out
    }

    fn prune_bucket(
        bucket: &mut Vec<RollupTopKCandidate>,
        sort_fields: &[crate::engine::executor::sort::SortField],
        k: usize,
        kind: crate::engine::executor::dispatch::RankKind,
    ) -> usize {
        if k == 0 {
            bucket.clear();
            return usize::MAX;
        }
        if bucket.len() <= k
            && !matches!(kind, crate::engine::executor::dispatch::RankKind::DenseRank)
        {
            return usize::MAX;
        }
        bucket.sort_by(|a, b| {
            crate::engine::executor::sort::compare_sort_keys(
                sort_fields,
                &a.order_key,
                &b.order_key,
            )
        });
        let mut boundary_ready = bucket.len() >= k;
        let keep = match kind {
            crate::engine::executor::dispatch::RankKind::RowNumber => bucket.len().min(k),
            crate::engine::executor::dispatch::RankKind::Rank => {
                if bucket.len() <= k {
                    bucket.len()
                } else {
                    let boundary = &bucket[k - 1].order_key;
                    bucket
                        .iter()
                        .take_while(|candidate| {
                            crate::engine::executor::sort::compare_sort_keys(
                                sort_fields,
                                &candidate.order_key,
                                boundary,
                            ) != std::cmp::Ordering::Greater
                        })
                        .count()
                }
            }
            crate::engine::executor::dispatch::RankKind::DenseRank => {
                let mut dense_rank = 0usize;
                let mut prev: Option<&[Value]> = None;
                let mut keep = 0usize;
                for candidate in bucket.iter() {
                    let new_group = prev
                        .map(|p| {
                            crate::engine::executor::sort::compare_sort_keys(
                                sort_fields,
                                p,
                                &candidate.order_key,
                            ) != std::cmp::Ordering::Equal
                        })
                        .unwrap_or(true);
                    if new_group {
                        dense_rank += 1;
                        prev = Some(candidate.order_key.as_slice());
                    }
                    if dense_rank > k {
                        break;
                    }
                    keep += 1;
                }
                boundary_ready = dense_rank >= k;
                keep
            }
        };
        bucket.truncate(keep);
        if boundary_ready && !bucket.is_empty() {
            bucket.len() - 1
        } else {
            usize::MAX
        }
    }
}

enum RollupEmitTarget {
    Rows(Vec<PlanRow>),
    TopK(RollupTopKCollector),
}

impl RollupEmitTarget {
    fn new(
        group_by: &[Expr],
        group_by_len: usize,
        slots: &[super::aggregate::AggregateSlot],
    ) -> Self {
        match current_rollup_window_topk_hint() {
            Some(hint) if hint.k > 0 && !hint.order_by.is_empty() => RollupEmitTarget::TopK(
                RollupTopKCollector::new(hint, group_by, group_by_len, slots),
            ),
            _ => RollupEmitTarget::Rows(Vec::new()),
        }
    }

    fn push_state(
        &mut self,
        slots: &[super::aggregate::AggregateSlot],
        group_by_len: usize,
        state: RollupGroupState,
        having: Option<&Expr>,
    ) -> Result<(), QueryError> {
        match self {
            RollupEmitTarget::Rows(rows) => {
                if let Some(row) = build_rollup_group_row(slots, group_by_len, state, having)? {
                    rows.push(row);
                }
                Ok(())
            }
            RollupEmitTarget::TopK(collector) => {
                collector.push_state(slots, group_by_len, state, having)
            }
        }
    }

    fn into_rows(self) -> Vec<PlanRow> {
        match self {
            RollupEmitTarget::Rows(rows) => rows,
            RollupEmitTarget::TopK(collector) => collector.into_rows(),
        }
    }
}

pub(in crate::engine::executor::select) fn rollup_slot_inputs(
    slots: &[super::aggregate::AggregateSlot],
    row: &PlanRow,
) -> Result<Vec<Option<Value>>, QueryError> {
    slots
        .iter()
        .map(|slot| {
            if slot.is_count_star {
                Ok(None)
            } else if slot.args.is_empty() {
                Ok(Some(Value::Null))
            } else {
                eval_plan_expr(&slot.args[0], row).map(Some)
            }
        })
        .collect()
}

pub(in crate::engine::executor::select) fn aggregate_slots_need_sample_fields(
    slots: &[super::aggregate::AggregateSlot],
) -> bool {
    slots.iter().any(|slot| {
        slot.name
            .eq_ignore_ascii_case(crate::parser::HIDDEN_FIRST_VALUE_FN)
    })
}

/// Feed one source row into a ROLLUP state while preserving aggregate-local
/// ordering. Every live ROLLUP path uses this helper so
/// `GROUP_CONCAT(... ORDER BY ...)` receives the per-row sort keys just as
/// ordinary hash/streaming aggregation does. Spill ROLLUP also retains the
/// original row while streaming its group-key-sorted input.
pub(in crate::engine::executor::select) fn update_rollup_accumulators_ordered(
    state: &mut RollupGroupState,
    slots: &[super::aggregate::AggregateSlot],
    inputs: &[Option<Value>],
    row: &PlanRow,
) -> Result<(), QueryError> {
    for ((slot, acc), input) in slots
        .iter()
        .zip(state.accumulators.iter_mut())
        .zip(inputs.iter())
    {
        if slot.is_count_star {
            acc.update_count_star();
        } else if let Some(value) = input {
            if slot.has_order_by() {
                let keys = slot.eval_order_keys(|expr| eval_plan_expr(expr, row))?;
                acc.update_ordered(value.clone(), keys)?;
            } else {
                acc.update_ref(value)?;
            }
        } else {
            acc.update(Value::Null)?;
        }
    }
    Ok(())
}

pub(in crate::engine::executor::select) fn merge_rollup_group_state(
    base: &mut RollupGroupState,
    mut other: RollupGroupState,
) {
    let drained: Vec<Box<dyn super::aggregate::Accumulator>> =
        std::mem::take(&mut other.accumulators);
    for (base_acc, worker_acc) in base.accumulators.iter_mut().zip(drained.into_iter()) {
        base_acc.merge_from(worker_acc.into_any());
    }
}

pub(in crate::engine::executor::select) fn emit_rollup_group_state(
    out: &mut Vec<PlanRow>,
    slots: &[super::aggregate::AggregateSlot],
    group_by_len: usize,
    state: RollupGroupState,
    having: Option<&Expr>,
) -> Result<(), QueryError> {
    if let Some(row) = build_rollup_group_row(slots, group_by_len, state, having)? {
        out.push(row);
    }
    Ok(())
}

/// Finalize a parallel merger's states through the same emission target used
/// by serial/vector ROLLUP. In particular, this preserves a surrounding
/// window RANK/DENSE_RANK Top-K hint instead of materializing every group into
/// rows before the window discards all but its prefix.
pub(in crate::engine::executor::select) fn emit_parallel_rollup_group_states(
    group_by: &[Expr],
    slots: &[super::aggregate::AggregateSlot],
    group_by_len: usize,
    states: Vec<RollupGroupState>,
    having: Option<&Expr>,
    topk_hint: Option<RollupWindowTopKHint>,
) -> Result<Vec<PlanRow>, QueryError> {
    let _topk_guard = super::set_rollup_window_topk_hint(topk_hint);
    let mut out = RollupEmitTarget::new(group_by, group_by_len, slots);
    for state in states {
        out.push_state(slots, group_by_len, state, having)?;
    }
    Ok(out.into_rows())
}

fn emit_rollup_group_state_to_target(
    out: &mut RollupEmitTarget,
    slots: &[super::aggregate::AggregateSlot],
    group_by_len: usize,
    state: RollupGroupState,
    having: Option<&Expr>,
) -> Result<(), QueryError> {
    out.push_state(slots, group_by_len, state, having)
}

fn build_rollup_group_row(
    slots: &[super::aggregate::AggregateSlot],
    group_by_len: usize,
    state: RollupGroupState,
    having: Option<&Expr>,
) -> Result<Option<PlanRow>, QueryError> {
    let RollupGroupState {
        prefix_len,
        key_values,
        sample_fields,
        accumulators,
    } = state;
    let mut cache: HashMap<u64, Value> = HashMap::with_capacity(slots.len() + 1);
    for (slot, acc) in slots.iter().zip(accumulators.into_iter()) {
        cache.insert(slot.key, acc.finalize()?);
    }
    cache.insert(
        super::aggregate::ROLLUP_MASK_AGG_KEY,
        Value::UInt(rollup_mask(group_by_len, prefix_len)),
    );
    let mut fields = sample_fields;
    fields.reserve(group_by_len);
    let mut key_values = key_values.into_iter();
    for idx in 0..group_by_len {
        let value = if idx < prefix_len {
            key_values.next().unwrap_or(Value::Null)
        } else {
            Value::Null
        };
        fields.push(super::aggregate::group_marker_field(idx, value));
    }
    let row = PlanRow {
        fields,
        group: Some(Vec::new()),
        agg_values: Some(std::sync::Arc::new(cache)),
        schema: PlanRow::empty_schema(),
    };
    if having
        .map(|expr| eval_aggregate_bool(expr, &[], &row))
        .transpose()?
        .unwrap_or(true)
    {
        Ok(Some(row))
    } else {
        Ok(None)
    }
}

fn build_compact_rollup_row(
    slots: &[super::aggregate::AggregateSlot],
    group_by_len: usize,
    prefix_len: usize,
    key_values: Vec<Value>,
    agg_values: Vec<Value>,
) -> PlanRow {
    let mut cache: HashMap<u64, Value> = HashMap::with_capacity(slots.len() + 1);
    for (slot, value) in slots.iter().zip(agg_values.into_iter()) {
        cache.insert(slot.key, value);
    }
    cache.insert(
        super::aggregate::ROLLUP_MASK_AGG_KEY,
        Value::UInt(rollup_mask(group_by_len, prefix_len)),
    );
    let mut fields = Vec::with_capacity(group_by_len);
    for idx in 0..group_by_len {
        let value = if idx < prefix_len {
            key_values.get(idx).cloned().unwrap_or(Value::Null)
        } else {
            Value::Null
        };
        fields.push(super::aggregate::group_marker_field(idx, value));
    }
    PlanRow {
        fields,
        group: Some(Vec::new()),
        agg_values: Some(std::sync::Arc::new(cache)),
        schema: PlanRow::empty_schema(),
    }
}

fn expr_matches_group_expr(expr: &Expr, group: &Expr) -> bool {
    fn unwrap(expr: &Expr) -> &Expr {
        match expr {
            Expr::GroupKeyRef { expr, .. } => expr,
            other => other,
        }
    }
    let expr = unwrap(expr);
    let group = unwrap(group);
    if crate::engine::expr_program::expr_semantically_eq(expr, group) {
        return true;
    }
    match (expr, group) {
        (Expr::Identifier(a), Expr::CompoundIdentifier(parts))
        | (Expr::CompoundIdentifier(parts), Expr::Identifier(a)) => {
            parts.last().is_some_and(|b| a.eq_ignore_ascii_case(b))
        }
        (Expr::Identifier(a), Expr::Identifier(b)) => a.eq_ignore_ascii_case(b),
        (Expr::CompoundIdentifier(a), Expr::CompoundIdentifier(b)) => a
            .last()
            .zip(b.last())
            .is_some_and(|(a, b)| a.eq_ignore_ascii_case(b)),
        _ => false,
    }
}

/// Two-phase ("subtotal-derived") in-memory ROLLUP.
///
/// The naive ROLLUP probes all `N+1` grouping-set prefixes for every input
/// row — `O(input × N)` hash operations, each re-cloning the (often long
/// string) prefix key. On a wide fact-table join (e.g. TPC-DS Q22
/// `inventory⋈date_dim⋈item`, ~2.4M rows × 4 string keys) that per-row
/// fan-out dominates the query.
///
/// Instead we exploit that ROLLUP's grouping sets are *prefix-nested*: a
/// coarser super-aggregate is the re-aggregation of the finer one. So:
///
///   Phase 1 — aggregate ONLY the finest set `(g0..g_{N-1})`: exactly one
///             hash probe per input row (identical to a plain `GROUP BY`).
///   Phase 2 — derive each coarser level `k` from level `k+1` by truncating
///             the key to its first `k` parts and `merge_from`-ing a clone of
///             the finer accumulators. Since `|level_{k+1}| ≤ |finest|` and
///             levels shrink upward, total work is `O(input + N × finest)`.
///
/// `merge_from`/`clone_box` are the same associative-monoid combine the
/// parallel aggregate already relies on, so this is correct for every
/// accumulator (AVG via (sum,count), COUNT DISTINCT via set union, variance
/// via n/sum/sum_sq, …). Emission order is irrelevant — the query's ORDER BY
/// runs downstream.
pub(in crate::engine::executor::select) fn aggregate_rollup_plan_rows(
    db: &Arc<Database>,
    input_rows: Vec<PlanRow>,
    group_by: &[Expr],
    having: Option<&Expr>,
    slots: &[super::aggregate::AggregateSlot],
) -> Result<Vec<PlanRow>, QueryError> {
    if slots.iter().any(|slot| {
        matches!(
            slot.name.to_ascii_uppercase().as_str(),
            "STD" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP" | "VAR_SAMP"
        )
    }) {
        // Welford's state is safely mergeable, but floating-point merge
        // grouping changes the last ULP relative to feeding the same rows in
        // scan order. The row oracle is the observable contract, so keep
        // variance-family ROLLUP on the per-row prefix path until subtotal
        // derivation can preserve that order exactly.
        return aggregate_rollup_plan_rows_per_row(input_rows, group_by, having, slots);
    }
    use crate::engine::expr::ValueKey;
    use hashbrown::hash_map::Entry;
    type RollupKey = smallvec::SmallVec<[ValueKey; 8]>;

    let group_by_len = group_by.len();
    // Estimation-independent spill guard (same pattern as the fused hash
    // aggregate): cap the in-memory finest map at the per-query budget's free
    // headroom captured now, minus a reserve so the spilling fallback still has
    // room. If the REAL accumulated group bytes would exceed it, abandon the
    // in-memory build and route the (already-materialized) input through the
    // sort-based spilling rollup. A wide join with few groups never trips this;
    // a genuinely high-cardinality rollup bails cleanly to spill rather than
    // OOMing — no reliance on a CBO cardinality estimate.
    let group_mem_cap = crate::engine::executor::mem_budget::current_budget()
        .map(|b| b.remaining().saturating_sub(b.limit() / 8))
        .unwrap_or(usize::MAX);
    // The representative row's full fields (`sample_fields`) are only consulted
    // by the MySQL "hidden column" extension — a SELECT/HAVING/ORDER BY column
    // that is neither a GROUP BY key nor an aggregate, which takes an arbitrary
    // row's value from the group. Group keys resolve via the `GroupKeyRef`
    // markers and aggregates via the agg cache, so when no hidden columns are
    // present the per-group full-row clone is pure waste. The planner appends a
    // `__hidden_first_value(col)` aggregate for every hidden column reference
    // (`append_hidden_group_column_aggregates`), so their presence is detectable
    // from the collected slots.
    let needs_sample_fields = aggregate_slots_need_sample_fields(slots);
    let per_group_bytes = 64 + slots.len() * 32;
    let mut group_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    let mut out = RollupEmitTarget::new(group_by, group_by_len, slots);

    // ROLLUP always includes the `()` grouping set (the grand total). Over an
    // empty input MySQL still emits that single super-aggregate row (e.g.
    // `NULL … NULL 0`); the per-row loop never runs, so seed it explicitly.
    // Partial-prefix groups (prefix_len >= 1) correctly stay absent: there are
    // no key values to roll up.
    if input_rows.is_empty() {
        let state = RollupGroupState {
            prefix_len: 0,
            key_values: Vec::new(),
            sample_fields: Vec::new(),
            accumulators: slots.iter().map(|slot| slot.make_accumulator()).collect(),
        };
        emit_rollup_group_state_to_target(&mut out, slots, group_by_len, state, having)?;
        return Ok(out.into_rows());
    }

    type LevelMap = hashbrown::HashMap<RollupKey, RollupGroupState, ahash::RandomState>;
    let collations: Vec<_> = group_by
        .iter()
        .map(|expr| super::eval::plan_expr_collation(expr, &input_rows[0]).map(|c| c.id))
        .collect();
    let input_schema = super::eval::input_schema_for_plan_row(&input_rows[0]);
    let compiled_group_by: Vec<super::eval::CompiledPlanExpr> = group_by
        .iter()
        .map(|expr| super::eval::CompiledPlanExpr::compile_once(expr, &input_schema))
        .collect();
    let compiled_slot_inputs: Vec<Option<super::eval::CompiledPlanExpr>> = slots
        .iter()
        .map(|slot| {
            if slot.is_count_star || slot.args.is_empty() {
                None
            } else {
                slot.args
                    .first()
                    .map(|expr| super::eval::CompiledPlanExpr::compile_once(expr, &input_schema))
            }
        })
        .collect();

    // ---- Phase 1: aggregate the finest grouping set (one probe per row). ----
    let mut finer: LevelMap = hashbrown::HashMap::with_hasher(ahash::RandomState::new());
    for row in &input_rows {
        crate::engine::cancel::check_cancelled()?;
        let full_values = compiled_group_by
            .iter()
            .map(|expr| expr.eval(row))
            .collect::<Result<Vec<_>, _>>()?;
        let slot_inputs = slots
            .iter()
            .zip(compiled_slot_inputs.iter())
            .map(|(slot, compiled)| {
                if slot.is_count_star {
                    Ok(None)
                } else if slot.args.is_empty() {
                    Ok(Some(Value::Null))
                } else if let Some(compiled) = compiled {
                    compiled.eval(row).map(Some)
                } else {
                    eval_plan_expr(&slot.args[0], row).map(Some)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let key: RollupKey = full_values
            .iter()
            .zip(&collations)
            .map(|(v, c)| ValueKey::from_with_collation(v, *c))
            .collect();
        match finer.entry(key) {
            Entry::Occupied(mut e) => {
                update_rollup_accumulators_ordered(e.get_mut(), slots, &slot_inputs, row)?;
            }
            Entry::Vacant(e) => {
                let charge = e
                    .key()
                    .iter()
                    .map(|vk| 24 + vk.payload_size())
                    .sum::<usize>()
                    + per_group_bytes;
                if !group_charge.add_within(charge, group_mem_cap) {
                    // Too many distinct finest groups to hold: drop the partial
                    // build (charge released on `group_charge` drop) and stream
                    // the full input through the sort-based spilling rollup.
                    drop(finer);
                    drop(group_charge);
                    return aggregate_rollup_plan_rows_spilled(
                        db, input_rows, group_by, having, slots,
                    );
                }
                let mut state = RollupGroupState {
                    prefix_len: group_by_len,
                    key_values: full_values,
                    sample_fields: if needs_sample_fields {
                        row.fields.clone()
                    } else {
                        Vec::new()
                    },
                    accumulators: slots.iter().map(|slot| slot.make_accumulator()).collect(),
                };
                update_rollup_accumulators_ordered(&mut state, slots, &slot_inputs, row)?;
                e.insert(state);
            }
        }
    }

    // ---- Phase 2: derive coarser prefix levels from the finest, top-down. ----
    // Each level `k` (group_by_len-1 down to 0) is derived from the already-built
    // `finer` level `k+1` by truncating the key to its first `k` parts; deriving
    // from `k+1` (not always the finest) keeps the source small as levels shrink.
    // Levels are collected and emitted at the end so that a mid-derivation spill
    // bail can hand the still-intact input to the spilling path with no
    // partially-emitted output.
    let mut levels: Vec<LevelMap> = Vec::with_capacity(group_by_len + 1);
    for prefix_len in (0..group_by_len).rev() {
        let mut level: LevelMap =
            hashbrown::HashMap::with_capacity_and_hasher(finer.len(), ahash::RandomState::new());
        // Hash-map iteration is deliberately randomized. Merge finer groups
        // in SQL key order so order-sensitive accumulator state remains
        // repeatable even when the aggregate itself has no ORDER BY. This
        // does not assign SQL semantics to an unspecified GROUP_CONCAT order;
        // it enforces the executor's stable-plan invariant and keeps triage
        // fingerprints reproducible.
        let mut finer_states = finer.values().collect::<Vec<_>>();
        sort_rollup_states(&mut finer_states)?;
        for finer_state in finer_states {
            let key: RollupKey = finer_state
                .key_values
                .iter()
                .take(prefix_len)
                .zip(&collations)
                .map(|(v, c)| ValueKey::from_with_collation(v, *c))
                .collect();
            match level.entry(key) {
                Entry::Occupied(mut e) => {
                    let dst = e.get_mut();
                    for (acc, src) in dst
                        .accumulators
                        .iter_mut()
                        .zip(finer_state.accumulators.iter())
                    {
                        acc.merge_from(src.clone_box().into_any());
                    }
                }
                Entry::Vacant(e) => {
                    let charge = e
                        .key()
                        .iter()
                        .map(|vk| 24 + vk.payload_size())
                        .sum::<usize>()
                        + per_group_bytes;
                    if !group_charge.add_within(charge, group_mem_cap) {
                        drop(level);
                        drop(levels);
                        drop(finer);
                        drop(group_charge);
                        return aggregate_rollup_plan_rows_spilled(
                            db, input_rows, group_by, having, slots,
                        );
                    }
                    e.insert(RollupGroupState {
                        prefix_len,
                        key_values: finer_state.key_values[..prefix_len].to_vec(),
                        sample_fields: if needs_sample_fields {
                            finer_state.sample_fields.clone()
                        } else {
                            Vec::new()
                        },
                        accumulators: finer_state
                            .accumulators
                            .iter()
                            .map(|a| a.clone_box())
                            .collect(),
                    });
                }
            }
        }
        levels.push(std::mem::replace(&mut finer, level));
    }
    // `finer` now holds the last level (the grand total `()`, or — when
    // group_by_len == 0 — the single finest group, which is itself `()`).
    levels.push(finer);
    for level in levels {
        let mut states = level.into_values().collect::<Vec<_>>();
        sort_rollup_states(&mut states)?;
        for state in states {
            emit_rollup_group_state_to_target(&mut out, slots, group_by_len, state, having)?;
        }
    }
    Ok(out.into_rows())
}

/// Pre-optimization per-row ROLLUP: build all `N+1` grouping-set prefixes by
/// probing each prefix for every input row. Quadratic-ish per-row fan-out
/// (the cost the two-phase [`aggregate_rollup_plan_rows`] eliminates); retained
/// only for order-sensitive floating-point variance states whose merge tree
/// would otherwise change the observable last ULP.
fn aggregate_rollup_plan_rows_per_row(
    input_rows: Vec<PlanRow>,
    group_by: &[Expr],
    having: Option<&Expr>,
    slots: &[super::aggregate::AggregateSlot],
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::expr::ValueKey;
    use hashbrown::hash_map::Entry;

    let group_by_len = group_by.len();
    let needs_sample_fields = aggregate_slots_need_sample_fields(slots);
    let mut groups: hashbrown::HashMap<
        (usize, Vec<ValueKey>),
        RollupGroupState,
        ahash::RandomState,
    > = hashbrown::HashMap::with_hasher(ahash::RandomState::new());
    let per_group_bytes = 64 + slots.len() * 32;
    let mut group_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    if input_rows.is_empty() {
        groups.insert(
            (0, Vec::new()),
            RollupGroupState {
                prefix_len: 0,
                key_values: Vec::new(),
                sample_fields: Vec::new(),
                accumulators: slots.iter().map(|slot| slot.make_accumulator()).collect(),
            },
        );
    }
    for row in &input_rows {
        crate::engine::cancel::check_cancelled()?;
        let full_values = group_by
            .iter()
            .map(|expr| eval_plan_expr(expr, row))
            .collect::<Result<Vec<_>, _>>()?;
        let slot_inputs = rollup_slot_inputs(slots, row)?;
        let full_keys: Vec<ValueKey> = full_values
            .iter()
            .zip(group_by)
            .map(|(v, expr)| {
                ValueKey::from_with_collation(
                    v,
                    super::eval::plan_expr_collation(expr, &row).map(|c| c.id),
                )
            })
            .collect();
        for prefix_len in 0..=group_by_len {
            let key: Vec<ValueKey> = full_keys[..prefix_len].to_vec();
            let charge =
                key.iter().map(|vk| 24 + vk.payload_size()).sum::<usize>() + per_group_bytes;
            match groups.entry((prefix_len, key)) {
                Entry::Occupied(mut e) => {
                    update_rollup_accumulators_ordered(e.get_mut(), slots, &slot_inputs, row)?;
                }
                Entry::Vacant(e) => {
                    group_charge.add_or_fallback(charge)?;
                    let mut state = RollupGroupState {
                        prefix_len,
                        key_values: full_values[..prefix_len].to_vec(),
                        sample_fields: if needs_sample_fields {
                            row.fields.clone()
                        } else {
                            Vec::new()
                        },
                        accumulators: slots.iter().map(|slot| slot.make_accumulator()).collect(),
                    };
                    update_rollup_accumulators_ordered(&mut state, slots, &slot_inputs, row)?;
                    e.insert(state);
                }
            }
        }
    }
    let mut states = groups.into_values().collect::<Vec<_>>();
    states.sort_by(|left, right| right.prefix_len.cmp(&left.prefix_len));
    let mut start = 0;
    while start < states.len() {
        let prefix_len = states[start].prefix_len;
        let end = states[start..]
            .iter()
            .position(|state| state.prefix_len != prefix_len)
            .map_or(states.len(), |offset| start + offset);
        sort_rollup_states(&mut states[start..end])?;
        start = end;
    }
    let mut out = Vec::with_capacity(states.len());
    for state in states {
        emit_rollup_group_state(&mut out, slots, group_by_len, state, having)?;
    }
    Ok(out)
}

/// Hash aggregation deliberately uses randomized maps, but ROLLUP is emitted
/// as an ordered sequence of grouping levels. Sort the states within each
/// level by their SQL group-key values so row/vector/spill paths agree and the
/// output does not depend on hash seeds. Any comparison failure is preserved
/// rather than silently choosing an arbitrary order.
fn sort_rollup_states<T: std::borrow::Borrow<RollupGroupState>>(
    states: &mut [T],
) -> Result<(), QueryError> {
    let field = crate::engine::executor::sort::SortField {
        ascending: true,
        nulls_first: true,
        collation_id: Some(crate::charset::default_collation_info().id),
    };
    states.sort_by(|left, right| {
        let left = left.borrow();
        let right = right.borrow();
        for (left_value, right_value) in left.key_values.iter().zip(&right.key_values) {
            let ordering = crate::engine::executor::sort::compare_value_for_sort(
                left_value,
                right_value,
                field,
            );
            if ordering != std::cmp::Ordering::Equal {
                return ordering;
            }
        }
        left.key_values.len().cmp(&right.key_values.len())
    });
    Ok(())
}

/// Sort-based, memory-bounded ROLLUP. Spills the sort to disk
/// (`sort_plan_rows_stream`) and streams the sorted rows, holding only one
/// in-flight accumulator set per prefix level — O(group_by_len) state —
/// instead of the full `(N+1) × distinct-groups` HashMap that
/// [`aggregate_rollup_plan_rows`] builds. Used when that in-memory map
/// would exceed the per-query budget (`should_spill_row_hash_agg`), so a
/// high-cardinality `GROUP BY … WITH ROLLUP` (TPC-DS Q64/Q67: 8-column
/// keys over a fact-table join) completes by spilling rather than 1037.
///
/// Correctness rests on the standard sort-aggregation invariant: rows that
/// share a prefix key are adjacent in the sorted stream, so a level-k group
/// closes exactly when the k-prefix changes (detected via the longest common
/// prefix of consecutive `ValueKey`s). Emission order is irrelevant — the
/// query's ORDER BY runs downstream.
pub(in crate::engine::executor::select) fn aggregate_rollup_plan_rows_spilled(
    db: &Arc<Database>,
    input_rows: Vec<PlanRow>,
    group_by: &[Expr],
    having: Option<&Expr>,
    slots: &[super::aggregate::AggregateSlot],
) -> Result<Vec<PlanRow>, QueryError> {
    aggregate_rollup_plan_stream_spilled(
        db,
        Box::new(MaterializedStream::new(input_rows)),
        group_by,
        having,
        slots,
    )
}

pub(in crate::engine::executor::select) fn aggregate_rollup_plan_stream_spilled<'a>(
    db: &Arc<Database>,
    input: Box<dyn PlanStream + 'a>,
    group_by: &[Expr],
    having: Option<&Expr>,
    slots: &[super::aggregate::AggregateSlot],
) -> Result<Vec<PlanRow>, QueryError> {
    aggregate_rollup_plan_stream_spilled_with_degree(db, input, group_by, having, slots, 1)
}

pub(in crate::engine::executor::select) fn aggregate_rollup_plan_stream_spilled_with_degree<'a>(
    db: &Arc<Database>,
    input: Box<dyn PlanStream + 'a>,
    group_by: &[Expr],
    having: Option<&Expr>,
    slots: &[super::aggregate::AggregateSlot],
    degree: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    // Sort by the full group key (ascending). NULL placement is immaterial:
    // equal keys are adjacent regardless of NULLS FIRST/LAST.
    let order_by: Vec<OrderBy> = group_by
        .iter()
        .map(|expr| OrderBy {
            expr: expr.clone(),
            asc: true,
            nulls_first: None,
        })
        .collect();
    let stream = super::external_sort::sort_plan_input_stream_with_parallel_merge(
        db.data_dir(),
        input,
        &order_by,
        degree,
    )?;
    aggregate_rollup_plan_stream_sorted(stream, group_by, having, slots)
}

pub(in crate::engine::executor::select) fn aggregate_rollup_plan_stream_sorted(
    mut stream: Box<dyn PlanStream + '_>,
    group_by: &[Expr],
    having: Option<&Expr>,
    slots: &[super::aggregate::AggregateSlot],
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::expr::ValueKey;
    let group_by_len = group_by.len();
    let needs_sample_fields = aggregate_slots_need_sample_fields(slots);
    let mut out = RollupEmitTarget::new(group_by, group_by_len, slots);

    // One accumulator set per prefix level 0..=N. Level 0 (grand total) never
    // closes until end-of-input; level k closes when the k-prefix changes.
    let mut level_states: Vec<Option<RollupGroupState>> =
        (0..=group_by_len).map(|_| None).collect();
    let mut prev_full: Option<Vec<ValueKey>> = None;
    let mut prepared = None;
    while let Some(row) = stream.next()? {
        crate::engine::cancel::check_cancelled()?;
        let (group_exprs, slot_exprs, collations) = prepared.get_or_insert_with(|| {
            let schema = super::eval::input_schema_for_plan_row(&row);
            let groups: Vec<_> = group_by
                .iter()
                .map(|expr| super::eval::CompiledPlanExpr::compile_once(expr, &schema))
                .collect();
            let args: Vec<_> = slots
                .iter()
                .map(|slot| {
                    if slot.is_count_star {
                        None
                    } else {
                        slot.args
                            .first()
                            .map(|expr| super::eval::CompiledPlanExpr::compile_once(expr, &schema))
                    }
                })
                .collect();
            let collations: Vec<_> = group_by
                .iter()
                .map(|expr| super::eval::plan_expr_collation(expr, &row).map(|c| c.id))
                .collect();
            (groups, args, collations)
        });
        let full_values = group_exprs
            .iter()
            .map(|expr| expr.eval(&row))
            .collect::<Result<Vec<_>, _>>()?;
        let full_keys: Vec<ValueKey> = full_values
            .iter()
            .zip(collations.iter())
            .map(|(v, collation)| ValueKey::from_with_collation(v, *collation))
            .collect();
        let slot_inputs = slots
            .iter()
            .zip(slot_exprs.iter())
            .map(|(slot, expr)| {
                if slot.is_count_star {
                    Ok(None)
                } else {
                    expr.as_ref()
                        .map_or(Ok(Some(Value::Null)), |expr| expr.eval(&row).map(Some))
                }
            })
            .collect::<Result<Vec<_>, QueryError>>()?;
        // Longest common prefix with the previous row's key. Levels deeper
        // than the LCP have a new prefix value → their old group is complete.
        let lcp = match &prev_full {
            Some(prev) => {
                let mut n = 0;
                while n < group_by_len && prev[n] == full_keys[n] {
                    n += 1;
                }
                n
            }
            None => group_by_len,
        };
        if prev_full.is_some() {
            for level in (lcp + 1..=group_by_len).rev() {
                if let Some(state) = level_states[level].take() {
                    emit_rollup_group_state_to_target(
                        &mut out,
                        slots,
                        group_by_len,
                        state,
                        having,
                    )?;
                }
            }
        }
        for level in 0..=group_by_len {
            if level_states[level].is_none() {
                level_states[level] = Some(RollupGroupState {
                    prefix_len: level,
                    key_values: full_values[..level].to_vec(),
                    sample_fields: if needs_sample_fields {
                        row.fields.clone()
                    } else {
                        Vec::new()
                    },
                    accumulators: slots.iter().map(|slot| slot.make_accumulator()).collect(),
                });
            }
            update_rollup_accumulators_ordered(
                level_states[level]
                    .as_mut()
                    .expect("level state just ensured"),
                slots,
                &slot_inputs,
                &row,
            )?;
        }
        prev_full = Some(full_keys);
    }
    for level in (0..=group_by_len).rev() {
        if let Some(state) = level_states[level].take() {
            emit_rollup_group_state_to_target(&mut out, slots, group_by_len, state, having)?;
        }
    }
    if prev_full.is_none() {
        let state = RollupGroupState {
            prefix_len: 0,
            key_values: Vec::new(),
            sample_fields: Vec::new(),
            accumulators: slots.iter().map(|slot| slot.make_accumulator()).collect(),
        };
        emit_rollup_group_state_to_target(&mut out, slots, group_by_len, state, having)?;
    }
    Ok(out.into_rows())
}

pub(in crate::engine::executor::select) fn execute_hash_grouping_sets_aggregate(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    group_by: &[Expr],
    grouping_sets: &[Vec<usize>],
    having: Option<&Expr>,
    aggregates: &[Expr],
) -> Result<Vec<PlanRow>, QueryError> {
    use super::aggregate::{collect_aggregate_slots, AggregateSlot};
    let mut slot_exprs: Vec<&Expr> = aggregates.iter().collect();
    if let Some(h) = having {
        slot_exprs.push(h);
    }
    let mut slots: Vec<AggregateSlot> = collect_aggregate_slots(&slot_exprs);
    super::aggregate::bind_slot_comparisons(&mut slots, input);
    if let Some(finest) =
        super::vec_bridge::vec_rollup_finest_groups(db, txn, input, group_by, &slots)?
    {
        return grouping_sets_emit_from_vec_finest(
            finest,
            group_by.len(),
            grouping_sets,
            having,
            &slots,
        );
    }
    let input_rows = execute_plan_rows(db, txn, input)?;
    aggregate_grouping_sets_plan_rows(input_rows, group_by, grouping_sets, having, &slots)
}

/// Build the finest grouping (group by ALL `group_by` columns) once, with one
/// hash probe per input row. Every GROUPING SET's columns are a subset of the
/// full column list, so the all-columns grouping is a common refinement of all
/// sets — each set is then derived from it by re-keying on the set's columns and
/// merging (see [`aggregate_grouping_sets_plan_rows`]). Shared finest builder
/// for the two-phase grouping-sets path.
struct FinestGroupState {
    key_values: Vec<Value>,
    sample_fields: Vec<PlanField>,
    accumulators: Vec<Box<dyn super::aggregate::Accumulator>>,
}

fn grouping_sets_emit_from_vec_finest(
    finest: super::vec_bridge::VecRollupFinest,
    group_by_len: usize,
    grouping_sets: &[Vec<usize>],
    having: Option<&Expr>,
    slots: &[super::aggregate::AggregateSlot],
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::expr::ValueKey;

    // Preserve the established empty-input behavior for GROUPING SETS with
    // non-empty grouping expressions: no finest groups means no output. Most
    // importantly, the already-consumed batch input is not reopened.
    if finest.groups.is_empty() {
        return Ok(Vec::new());
    }
    let mut groups: hashbrown::HashMap<Vec<ValueKey>, FinestGroupState, ahash::RandomState> =
        hashbrown::HashMap::with_hasher(ahash::RandomState::new());
    for group in finest.groups {
        let key = group
            .key_values
            .iter()
            .zip(&finest.collations)
            .map(|(v, c)| ValueKey::from_with_collation(v, *c))
            .collect();
        let accumulators = slots
            .iter()
            .zip(group.agg_states.iter())
            .map(|(slot, state)| {
                super::aggregate::seed_rollup_accumulator(
                    slot,
                    &state.sum,
                    state.count,
                    &state.min,
                    &state.max,
                )
            })
            .collect();
        groups.insert(
            key,
            FinestGroupState {
                key_values: group.key_values,
                sample_fields: Vec::new(),
                accumulators,
            },
        );
    }
    let mut group_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    derive_grouping_sets_from_finest(
        &groups,
        group_by_len,
        grouping_sets,
        having,
        slots,
        &mut group_charge,
    )
}

pub(in crate::engine::executor::select) fn aggregate_grouping_sets_plan_rows(
    input_rows: Vec<PlanRow>,
    group_by: &[Expr],
    grouping_sets: &[Vec<usize>],
    having: Option<&Expr>,
    slots: &[super::aggregate::AggregateSlot],
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::expr::ValueKey;
    use hashbrown::hash_map::Entry;

    let group_by_len = group_by.len();
    let grouping_sets_owned;
    let grouping_sets = if grouping_sets.is_empty() {
        grouping_sets_owned = vec![(0..group_by_len).collect::<Vec<_>>()];
        grouping_sets_owned.as_slice()
    } else {
        grouping_sets
    };
    let needs_sample_fields = aggregate_slots_need_sample_fields(slots);
    let per_group_bytes = 64 + slots.len() * 32;
    let mut group_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    let mut out = Vec::new();

    // Empty input (preserve the pre-optimization behavior exactly): only when
    // there are no GROUP BY columns at all does each grouping set emit its
    // single super-aggregate `()` row; otherwise nothing is emitted.
    if input_rows.is_empty() {
        if group_by.is_empty() {
            for set_indices in grouping_sets.iter() {
                let state = GroupingSetGroupState {
                    set_indices: set_indices.clone(),
                    group_values: Vec::new(),
                    sample_fields: Vec::new(),
                    accumulators: slots.iter().map(|slot| slot.make_accumulator()).collect(),
                };
                emit_grouping_set_group_state(&mut out, slots, group_by_len, state, having)?;
            }
        }
        return Ok(out);
    }

    // ---- Phase 1: aggregate the finest grouping (ALL columns), one probe/row.
    let collations: Vec<_> = group_by
        .iter()
        .map(|expr| super::eval::plan_expr_collation(expr, &input_rows[0]).map(|c| c.id))
        .collect();
    let input_schema = super::eval::input_schema_for_plan_row(&input_rows[0]);
    let compiled_group_by: Vec<super::eval::CompiledPlanExpr> = group_by
        .iter()
        .map(|expr| super::eval::CompiledPlanExpr::compile_once(expr, &input_schema))
        .collect();
    let compiled_slot_inputs: Vec<Option<super::eval::CompiledPlanExpr>> = slots
        .iter()
        .map(|slot| {
            if slot.is_count_star || slot.args.is_empty() {
                None
            } else {
                slot.args
                    .first()
                    .map(|expr| super::eval::CompiledPlanExpr::compile_once(expr, &input_schema))
            }
        })
        .collect();
    let mut finest: hashbrown::HashMap<Vec<ValueKey>, FinestGroupState, ahash::RandomState> =
        hashbrown::HashMap::with_hasher(ahash::RandomState::new());
    for row in &input_rows {
        crate::engine::cancel::check_cancelled()?;
        let full_values = compiled_group_by
            .iter()
            .map(|expr| expr.eval(row))
            .collect::<Result<Vec<_>, _>>()?;
        let slot_inputs = slots
            .iter()
            .zip(compiled_slot_inputs.iter())
            .map(|(slot, compiled)| {
                if slot.is_count_star {
                    Ok(None)
                } else if slot.args.is_empty() {
                    Ok(Some(Value::Null))
                } else if let Some(compiled) = compiled {
                    compiled.eval(row).map(Some)
                } else {
                    eval_plan_expr(&slot.args[0], row).map(Some)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let key: Vec<ValueKey> = full_values
            .iter()
            .zip(&collations)
            .map(|(v, c)| ValueKey::from_with_collation(v, *c))
            .collect();
        match finest.entry(key) {
            Entry::Occupied(mut e) => {
                update_finest_accumulators(&mut e.get_mut().accumulators, slots, &slot_inputs)?;
            }
            Entry::Vacant(e) => {
                let charge = e
                    .key()
                    .iter()
                    .map(|vk| 24 + vk.payload_size())
                    .sum::<usize>()
                    + per_group_bytes;
                group_charge.add_or_fallback(charge)?;
                let mut accumulators: Vec<_> =
                    slots.iter().map(|slot| slot.make_accumulator()).collect();
                update_finest_accumulators(&mut accumulators, slots, &slot_inputs)?;
                e.insert(FinestGroupState {
                    key_values: full_values,
                    sample_fields: if needs_sample_fields {
                        row.fields.clone()
                    } else {
                        Vec::new()
                    },
                    accumulators,
                });
            }
        }
    }

    derive_grouping_sets_from_finest(
        &finest,
        group_by_len,
        grouping_sets,
        having,
        slots,
        &mut group_charge,
    )
}

fn derive_grouping_sets_from_finest(
    finest: &hashbrown::HashMap<
        Vec<crate::engine::expr::ValueKey>,
        FinestGroupState,
        ahash::RandomState,
    >,
    group_by_len: usize,
    grouping_sets: &[Vec<usize>],
    having: Option<&Expr>,
    slots: &[super::aggregate::AggregateSlot],
    group_charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::expr::ValueKey;
    use hashbrown::hash_map::Entry;

    let per_group_bytes = 64 + slots.len() * 32;
    let needs_sample_fields = aggregate_slots_need_sample_fields(slots);
    let mut out = Vec::new();
    // Derive each grouping set from the common finest groups. Each set is
    // keyed independently, so duplicate sets (for example `(a),(a)`) retain
    // their SQL multiplicity.
    // Each set is keyed independently (by its own column projection), so
    // duplicate sets (e.g. GROUPING SETS ((a),(a))) each emit their own row,
    // exactly as the per-row path did via the `set_ordinal` key component.
    for set_indices in grouping_sets.iter() {
        let mut level: hashbrown::HashMap<
            Vec<ValueKey>,
            GroupingSetGroupState,
            ahash::RandomState,
        > = hashbrown::HashMap::with_hasher(ahash::RandomState::new());
        for (finest_key, finest_state) in finest {
            let key: Vec<ValueKey> = set_indices
                .iter()
                .filter_map(|idx| finest_key.get(*idx).cloned())
                .collect();
            match level.entry(key) {
                Entry::Occupied(mut e) => {
                    let dst = e.get_mut();
                    for (acc, src) in dst
                        .accumulators
                        .iter_mut()
                        .zip(finest_state.accumulators.iter())
                    {
                        acc.merge_from(src.clone_box().into_any());
                    }
                }
                Entry::Vacant(e) => {
                    let charge = e
                        .key()
                        .iter()
                        .map(|vk| 24 + vk.payload_size())
                        .sum::<usize>()
                        + per_group_bytes;
                    group_charge.add_or_fallback(charge)?;
                    let mut group_values = vec![Value::Null; group_by_len];
                    for idx in set_indices {
                        if let Some(value) = finest_state.key_values.get(*idx) {
                            group_values[*idx] = value.clone();
                        }
                    }
                    e.insert(GroupingSetGroupState {
                        set_indices: set_indices.clone(),
                        group_values,
                        sample_fields: if needs_sample_fields {
                            finest_state.sample_fields.clone()
                        } else {
                            Vec::new()
                        },
                        accumulators: finest_state
                            .accumulators
                            .iter()
                            .map(|a| a.clone_box())
                            .collect(),
                    });
                }
            }
        }
        for (_, state) in level {
            emit_grouping_set_group_state(&mut out, slots, group_by_len, state, having)?;
        }
    }
    Ok(out)
}

fn update_finest_accumulators(
    accumulators: &mut [Box<dyn super::aggregate::Accumulator>],
    slots: &[super::aggregate::AggregateSlot],
    inputs: &[Option<Value>],
) -> Result<(), QueryError> {
    for ((slot, acc), input) in slots.iter().zip(accumulators.iter_mut()).zip(inputs.iter()) {
        if slot.is_count_star {
            acc.update_count_star();
        } else if let Some(value) = input {
            acc.update_ref(value)?;
        } else {
            acc.update(Value::Null)?;
        }
    }
    Ok(())
}

fn emit_grouping_set_group_state(
    out: &mut Vec<PlanRow>,
    slots: &[super::aggregate::AggregateSlot],
    group_by_len: usize,
    state: GroupingSetGroupState,
    having: Option<&Expr>,
) -> Result<(), QueryError> {
    let GroupingSetGroupState {
        set_indices,
        group_values,
        sample_fields,
        accumulators,
    } = state;
    let mut cache: HashMap<u64, Value> = HashMap::with_capacity(slots.len() + 1);
    for (slot, acc) in slots.iter().zip(accumulators.into_iter()) {
        cache.insert(slot.key, acc.finalize()?);
    }
    cache.insert(
        super::aggregate::ROLLUP_MASK_AGG_KEY,
        Value::UInt(grouping_set_mask(group_by_len, &set_indices)),
    );
    let mut fields = sample_fields;
    for idx in 0..group_by_len {
        fields.push(super::aggregate::group_marker_field(
            idx,
            group_values.get(idx).cloned().unwrap_or(Value::Null),
        ));
    }
    let row = PlanRow {
        fields,
        group: Some(Vec::new()),
        agg_values: Some(std::sync::Arc::new(cache)),
        schema: PlanRow::empty_schema(),
    };
    if having
        .map(|expr| eval_aggregate_bool(expr, &[], &row))
        .transpose()?
        .unwrap_or(true)
    {
        out.push(row);
    }
    Ok(())
}

pub(in crate::engine::executor::select) fn fixed_scale_approximate_aggregate_requires_row_oracle(
    input: &PhysicalPlan,
    slots: &[super::aggregate::AggregateSlot],
) -> bool {
    let mut fields = Vec::new();
    collect_plan_fields(input, &mut fields);
    slots.iter().any(|slot| {
        if !matches!(slot.name.to_ascii_uppercase().as_str(), "SUM" | "AVG") {
            return false;
        }
        let Some(arg) = slot.args.first() else {
            return false;
        };
        let (qualifier, column) = match arg {
            Expr::Identifier(column) => (None, column.as_str()),
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => (
                Some(parts[parts.len() - 2].as_str()),
                parts[parts.len() - 1].as_str(),
            ),
            _ => return false,
        };
        let mut matching = fields.iter().filter(|field| {
            field.column.eq_ignore_ascii_case(column)
                && qualifier.is_none_or(|qualifier| {
                    field
                        .alias
                        .as_deref()
                        .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier))
                        || field.table.eq_ignore_ascii_case(qualifier)
                })
        });
        let Some(field) = matching.next() else {
            return false;
        };
        matching.next().is_none()
            && matches!(field.column_type, ColumnType::Float | ColumnType::Double)
            && field
                .source
                .decimals
                .is_some_and(|decimals| decimals != 0x1f)
    })
}

#[cfg(test)]
mod bounded_rollup_input_tests {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};

    #[test]
    fn rollup_rank_boundaries_match_full_sort_with_large_peer_groups() {
        use crate::engine::executor::dispatch::RankKind;
        for kind in [RankKind::RowNumber, RankKind::Rank, RankKind::DenseRank] {
            for ascending in [true, false] {
                for nulls_first in [true, false] {
                    let mut collector = RollupTopKCollector::new(
                        RollupWindowTopKHint {
                            partition_by: Vec::new(),
                            order_by: vec![OrderBy {
                                expr: Expr::Identifier("score".into()),
                                asc: ascending,
                                nulls_first: Some(nulls_first),
                            }],
                            k: 3,
                            kind,
                        },
                        &[],
                        0,
                        &[],
                    );
                    let mut reference = [Vec::new(), Vec::new()];
                    for id in 0..1000usize {
                        let value = match id {
                            0..200 => Value::Int(10),
                            200..400 => Value::Int(11),
                            400..600 => Value::Int(12),
                            600..800 => Value::Int((id % 3) as i64),
                            _ => Value::Null,
                        };
                        reference[id % 2].push((id, value.clone()));
                        collector.push_candidate(
                            &[ValueKey::Int((id % 2) as i128)],
                            vec![value],
                            RollupTopKPayload::Row(PlanRow::from_fields(vec![PlanField {
                                meta: intern_col_meta(ColMeta {
                                    column: "id".into(),
                                    ..Default::default()
                                }),
                                value: Value::Int(id as i64),
                            }])),
                        );
                    }
                    let field = collector.sort_fields[0];
                    let mut expected = Vec::new();
                    for rows in &mut reference {
                        rows.sort_by(|a, b| {
                            crate::engine::executor::sort::compare_value_for_sort(&a.1, &b.1, field)
                        });
                        let mut rank = 0;
                        let mut dense = 0;
                        for index in 0..rows.len() {
                            if index == 0
                                || crate::engine::executor::sort::compare_value_for_sort(
                                    &rows[index - 1].1,
                                    &rows[index].1,
                                    field,
                                ) != std::cmp::Ordering::Equal
                            {
                                rank = index + 1;
                                dense += 1;
                            }
                            let keep = match kind {
                                RankKind::RowNumber => index < 3,
                                RankKind::Rank => rank <= 3,
                                RankKind::DenseRank => dense <= 3,
                            };
                            if keep {
                                expected.push(rows[index].0 as i64);
                            }
                        }
                    }
                    let mut actual = collector
                        .into_rows()
                        .into_iter()
                        .map(|row| match row.fields[0].value {
                            Value::Int(id) => id,
                            _ => unreachable!(),
                        })
                        .collect::<Vec<_>>();
                    actual.sort();
                    expected.sort();
                    assert_eq!(actual, expected);
                }
            }
        }
    }

    struct GeneratedRows {
        position: usize,
        count: usize,
        pulls: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl PlanStream for GeneratedRows {
        fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
            if self.position == self.count {
                return Ok(None);
            }
            let ordinal = self.position;
            self.position += 1;
            self.pulls.fetch_add(1, Ordering::Relaxed);
            Ok(Some(PlanRow::from_fields(vec![PlanField {
                meta: intern_col_meta(ColMeta {
                    column: "value".into(),
                    ..Default::default()
                }),
                value: Value::String(format!("{ordinal:06}{}", "x".repeat(1024))),
            }])))
        }
    }

    #[test]
    fn rollup_input_spills_a_bounded_prefix_without_reopening_or_losing_rows() {
        let budget = Arc::new(MemBudget::new(16 * 1024));
        let pulls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        with_query_budget(budget.clone(), || {
            let input = Box::new(GeneratedRows {
                position: 0,
                count: 1000,
                pulls: pulls.clone(),
            });
            let PreparedRollupInput::Stream(mut stream) = prepare_rollup_stream(input).unwrap()
            else {
                panic!("wide input must become a bounded stream");
            };
            assert!(pulls.load(Ordering::Relaxed) < 8);
            assert!(budget.used() > 0 && budget.used() <= budget.limit() / 4);
            for ordinal in 0..1000 {
                let row = stream.next().unwrap().unwrap();
                let Value::String(value) = &row.fields[0].value else {
                    panic!("string");
                };
                assert!(value.starts_with(&format!("{ordinal:06}")));
            }
            assert!(stream.next().unwrap().is_none());
        });
        assert_eq!(pulls.load(Ordering::Relaxed), 1000);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn dropping_unconsumed_rollup_prefix_returns_the_reservation() {
        let budget = Arc::new(MemBudget::new(16 * 1024));
        with_query_budget(budget.clone(), || {
            let input = Box::new(GeneratedRows {
                position: 0,
                count: 1000,
                pulls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            });
            let prepared = prepare_rollup_stream(input).unwrap();
            assert!(budget.used() > 0);
            drop(prepared);
            assert_eq!(budget.used(), 0);
        });
    }
}
