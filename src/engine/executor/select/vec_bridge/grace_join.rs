//! grace join execution and preparation.

use super::*;

/// Incremental reader for a grace-join value partition. Values are decoded
/// directly into the original typed lanes; at most one Batch is resident.
pub(super) struct VecJoinSpillBatchStream {
    pub(super) schema: Arc<BatchSchema>,
    pub(super) batch_rows: usize,
    pub(super) reader: Option<crate::engine::executor::select::grace::ValueRowReader>,
}

impl VecJoinSpillBatchStream {
    pub(super) fn open(
        path: &std::path::Path,
        schema: Arc<BatchSchema>,
        charger: crate::engine::executor::spill_quota::SpillCharger,
    ) -> Result<Self, QueryError> {
        let workers = scheduler_hash_join_workers().max(1);
        let batch_bytes = mem_budget::current_budget()
            .map(|budget| budget.limit() / workers / 8)
            .unwrap_or(1024 * 1024);
        // Tiny partitions must not reserve a full query-sized set of lanes.
        // The encoded file size is a stable upper bound on useful decoding
        // work, including long variable-width values, and avoids repeated
        // repartitioning caused solely by unused accumulator capacity.
        let batch_bytes = std::fs::metadata(path)
            .ok()
            .map(|metadata| batch_bytes.min(metadata.len().min(usize::MAX as u64) as usize))
            .unwrap_or(batch_bytes);
        let batch_rows =
            (batch_bytes / schema.fields().len().max(1) / std::mem::size_of::<Value>())
                .clamp(1, DEFAULT_BATCH_ROWS);
        let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(workers * 2);
        Ok(Self {
            schema,
            batch_rows,
            reader: crate::engine::executor::select::grace::ValueRowReader::open_with_capacity(
                path, charger, capacity,
            )?,
        })
    }
}

impl BatchOperator for VecJoinSpillBatchStream {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(None);
        };
        let batch_rows = reader.reserve_decode_batch(self.batch_rows)?;
        let mut columns = self
            .schema
            .fields()
            .iter()
            .map(|field| ColumnAccumulator::for_field(field, batch_rows))
            .collect::<Vec<_>>();
        crate::engine::cancel::check_cancelled()?;
        let rows = reader.read_accumulators(&mut columns, batch_rows)?;
        if rows == 0 {
            self.reader = None;
            return Ok(None);
        }
        Ok(Some(
            Batch::with_row_count(
                self.schema.clone(),
                columns.into_iter().map(ColumnAccumulator::finish).collect(),
                rows,
            )
            .with_memory_charge(reader.take_decode_charge())?,
        ))
    }
}

pub(super) fn vec_build_cap_exceeded(cap: vec_hash_join::BuildCap, rows: u64, bytes: u64) -> bool {
    (cap.max_rows > 0 && rows > cap.max_rows) || (cap.max_bytes > 0 && bytes > cap.max_bytes)
}

/// Pull build batches only until the in-memory cap is crossed. On overflow the
/// returned prefix includes the crossing batch and `build` remains positioned
/// at the next batch, allowing grace spill to continue without a second scan.
pub(super) fn drain_vec_join_build_to_cap(
    build: &mut dyn BatchOperator,
    cap: vec_hash_join::BuildCap,
    work_area: Option<&crate::engine::executor::work_area::WorkAreaHandle>,
) -> Result<
    (
        Vec<Batch>,
        bool,
        crate::engine::executor::mem_budget::ScopedCharge,
    ),
    QueryError,
> {
    drain_vec_join_build_to_cap_impl(build, cap, work_area, 0)
}

pub(super) fn drain_vec_join_build_to_cap_impl(
    build: &mut dyn BatchOperator,
    cap: vec_hash_join::BuildCap,
    work_area: Option<&crate::engine::executor::work_area::WorkAreaHandle>,
    replay_progress_bytes: usize,
) -> Result<
    (
        Vec<Batch>,
        bool,
        crate::engine::executor::mem_budget::ScopedCharge,
    ),
    QueryError,
> {
    let mut batches = Vec::new();
    let mut memory_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    // Pipeline borrowing can cover overlapping producers, but it must not
    // let this build alone consume more than the query's entire work area.
    // Otherwise a retained build can strand a later consumer at its first
    // allocation (even a tiny TopN), despite ample global executor headroom.
    let own_build_limit =
        mem_budget::current_budget().map(|budget| budget.limit().max(replay_progress_bytes));
    let mut rows = 0u64;
    let mut bytes = 0u64;
    while let Some(batch) = build.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        let batch_rows = batch.visible_rows() as u64;
        rows = rows.saturating_add(batch_rows);
        let batch_bytes = (batch.memory_bytes() as u64)
            .saturating_add(vec_hash_join::estimate_build_index_bytes(batch_rows));
        bytes = bytes.saturating_add(batch_bytes);
        let resident_bytes = batch_bytes.min(usize::MAX as u64) as usize;
        let memory_wants_spill = match memory_charge.add(resident_bytes) {
            Ok(()) => false,
            Err(_) => {
                // The crossing batch already belongs to this operator. Keep
                // it attributed while Grace writes it out, but force may only
                // cross soft watermarks; the unified hard target can still
                // terminate the SQL with 1037.
                memory_charge.add_force(resident_bytes)?;
                // A replay partition needs a bounded minimum work area even
                // when its blocking ancestors occupy the shared soft quota.
                // Repartitioning an already tiny input cannot release those
                // ancestors and only adds readers/recursion. The process hard
                // governor still admits every byte; larger inputs still spill.
                memory_charge.charged() > replay_progress_bytes
            }
        };
        batches.push(batch);
        let work_area_wants_spill = work_area.is_some_and(|handle| {
            matches!(
                handle.try_charge(batch_bytes.min(usize::MAX as u64) as usize),
                crate::engine::executor::work_area::SpillHint::Spill
            )
        });
        if vec_build_cap_exceeded(cap, rows, bytes)
            || own_build_limit.is_some_and(|limit| memory_charge.charged() > limit)
        {
            return Ok((batches, true, memory_charge));
        }
        if work_area_wants_spill || memory_wants_spill {
            if work_area.is_some_and(|handle| {
                matches!(
                    handle.try_borrow_for_pipeline_drain(),
                    crate::engine::executor::work_area::SpillHint::Continue
                )
            }) {
                // The dependent producer and this blocking build still fit in
                // the unchanged global executor target. Continue draining;
                // later charges re-check both fair pressure and the hard pool.
                continue;
            }
            return Ok((batches, true, memory_charge));
        }
    }
    Ok((batches, false, memory_charge))
}

pub(super) enum GraceProbeBloom {
    /// Collation-aware encoded join keys. Used when a scan-level ValueKey
    /// filter would not preserve the join's comparison semantics.
    Encoded(crate::engine::executor::select::bloom::BloomFilter),
    /// Per-component ordinary-key filters. All are pushed into the unopened
    /// scan and evaluated with short-circuit AND. Any full-key match must
    /// match every component, so each filter is independently safe.
    Components(Vec<(usize, crate::engine::executor::select::bloom::BloomFilter)>),
}

impl GraceProbeBloom {
    pub(super) fn insert(&mut self, batch: &Batch, keys: &[usize], row: usize, encoded: &[u8]) {
        match self {
            Self::Encoded(filter) => filter.insert(encoded),
            Self::Components(filters) => {
                filters.retain_mut(|(component, filter)| {
                    let column = &batch.columns()[keys[*component]];
                    if filter.uses_int_keys() {
                        if let Some(values) = column.as_i64_slice() {
                            filter.insert_int(i128::from(values[row]));
                        } else if let crate::engine::expr::ValueKey::Int(key) =
                            crate::engine::expr::ValueKey::from(&column.value_at(row))
                        {
                            filter.insert_int(key);
                        } else {
                            // Dictionary/coercion adapters need not retain the
                            // schema's physical lane. Drop an unrepresentable
                            // optional component instead of losing build keys.
                            return false;
                        }
                    } else {
                        let key = [crate::engine::expr::ValueKey::from(&column.value_at(row))];
                        filter.insert(&key[..]);
                    }
                    true
                });
            }
        }
    }

    pub(super) fn contains(
        &self,
        batch: &Batch,
        keys: &[usize],
        row: usize,
        encoded: &[u8],
    ) -> bool {
        match self {
            Self::Encoded(filter) => filter.contains(encoded),
            Self::Components(filters) => filters.iter().all(|(component, filter)| {
                let column = keys[*component];
                if filter.uses_int_keys() {
                    batch.columns()[column]
                        .as_i64_slice()
                        .map(|values| filter.contains_int(i128::from(values[row])))
                        // A physical lane mismatch cannot prove absence. Exact
                        // join comparison still checks every admitted row.
                        .unwrap_or_else(|| {
                            filter.contains_value_keys(&[crate::engine::expr::ValueKey::from(
                                &batch.columns()[column].value_at(row),
                            )])
                        })
                } else {
                    let key = [crate::engine::expr::ValueKey::from(
                        &batch.columns()[column].value_at(row),
                    )];
                    filter.contains(&key[..])
                }
            }),
        }
    }
}

/// Preserve key locality at the first Grace level instead of scattering
/// every physical key range across every partition. The bounded sample only
/// selects a radix window; exact canonical keys still decide equality in replay.
/// Recursive overflow retains hash repartitioning, including the established
/// skew/hot-key fallback. Non-integer equality classes retain hash routing.
pub(super) struct GraceRangePartitioning {
    pub(super) shift: u32,
    pub(super) mask: u128,
}

impl GraceRangePartitioning {
    pub(super) fn from_prefix(
        prefix: &[Batch],
        build_keys: &[usize],
        probe_keys: &[usize],
        null_safe: &[bool],
        collations: &[Option<u16>],
        partitions: usize,
        depth: u32,
    ) -> Option<Self> {
        if depth != 0
            || build_keys.len() != 1
            || probe_keys.len() != 1
            || !(2..=256).contains(&partitions)
            || !partitions.is_power_of_two()
        {
            return None;
        }
        if crate::util::env::var("MYDB_HASH_JOIN_RANGE_PARTITION").is_ok_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no"
            )
        }) {
            return None;
        }
        let rows = prefix.iter().map(Batch::visible_rows).sum::<usize>();
        let sample_cap = partitions.clamp(2, 256) * 32;
        if rows < partitions.saturating_mul(4) {
            return None;
        }
        let _charge = mem_budget::ScopedCharge::new_strict(
            (sample_cap + partitions) * std::mem::size_of::<i128>() + 256,
            "Grace range partition sample",
        )
        .ok()?;
        let mut sample = Vec::new();
        sample.try_reserve_exact(sample_cap).ok()?;
        let stride = rows.div_ceil(sample_cap).max(1);
        let mut skip = 0;
        let mut key = Vec::with_capacity(32);
        for batch in prefix {
            for row in batch.visible_indices() {
                if skip != 0 {
                    skip -= 1;
                    continue;
                }
                skip = stride - 1;
                // Do not allocate collation/LOB keys for an optional numeric
                // sample. Generic lanes can contain arbitrarily wide text;
                // only bounded numeric/temporal values need canonicalizing.
                let numeric = match &batch.columns()[build_keys[0]] {
                    ColumnVector::Null { .. }
                    | ColumnVector::Utf8 { .. }
                    | ColumnVector::Bytes { .. }
                    | ColumnVector::Json { .. }
                    | ColumnVector::Geometry { .. }
                    | ColumnVector::Vector { .. } => false,
                    ColumnVector::Generic { values } => match &values[row] {
                        Value::Null
                        | Value::String(_)
                        | Value::SmallString(_)
                        | Value::Bytes(_)
                        | Value::Json(_)
                        | Value::Geometry(_)
                        | Value::Vector(_) => false,
                        Value::Decimal(text) => text.len() <= 128,
                        _ => true,
                    },
                    _ => true,
                };
                if !numeric {
                    continue;
                }
                if vec_hash_join::encode_join_key_with_collations(
                    batch, build_keys, null_safe, collations, row, &mut key,
                ) {
                    if let Some(value) =
                        crate::engine::executor::vec::key_encode::single_integer_key(&key)
                    {
                        sample.push(value);
                    }
                }
            }
        }
        if sample.len() < partitions.saturating_mul(4) {
            return None;
        }
        let min = *sample.iter().min()?;
        let max = *sample.iter().max()?;
        let span = max.abs_diff(min);
        if span == 0 {
            return None;
        }
        let partition_bits = partitions.trailing_zeros();
        let max_shift = (u128::BITS - span.leading_zeros()).saturating_sub(partition_bits);
        let mask = (partitions - 1) as u128;
        let max_bucket = sample.len().div_ceil(partitions).saturating_mul(3);
        // The prefix is NOT a population CDF. Quantile boundaries can leave
        // huge holes which later rows fill, concentrating most of the build
        // into a few buckets. Fixed-width radix ranges cannot expand a bucket
        // to cover such a hole. Values outside the prefix cycle through the
        // same ranges instead of accumulating at an edge.
        // Start with the coarsest useful ranges. If outliers or gaps make
        // that window skewed, progressively use lower bits; only accept a
        // window whose observed load is bounded. True hot keys retain the
        // ordinary hash/repartition fallback.
        for shift in (0..=max_shift).rev() {
            let mut counts = [0usize; 256];
            for value in &sample {
                counts[(((*value as u128) >> shift) & mask) as usize] += 1;
            }
            if counts[..partitions].iter().copied().max()? <= max_bucket {
                return Some(Self { shift, mask });
            }
        }
        None
    }

    pub(super) fn partition(&self, key: &[u8]) -> Option<usize> {
        let value = crate::engine::executor::vec::key_encode::single_integer_key(key)?;
        // Truncating to a bit window is intentional, including for negative
        // values. Equal canonical keys always co-locate; wrapping only groups
        // additional ranges and never claims an SQL output ordering.
        Some((((value as u128) >> self.shift) & self.mask) as usize)
    }
}

pub(super) fn spill_vec_join_batch(
    batch: &Batch,
    keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
    writers: &mut crate::engine::executor::select::grace::LazyValueRowWriterPool,
    null_sequence: &mut u64,
    depth: u32,
    range: Option<&GraceRangePartitioning>,
    mut bloom: Option<&mut GraceProbeBloom>,
    filter_probe: bool,
) -> Result<u64, QueryError> {
    let partitions = writers.len();
    let mut key = Vec::with_capacity(64);
    let mut filtered = 0u64;
    let component_prefilter_available =
        filter_probe && matches!(bloom.as_deref(), Some(GraceProbeBloom::Components(_)));
    for row_idx in batch.visible_indices() {
        // Component filters operate on native typed lanes and do not need the
        // encoded composite key.  Probe them before key encoding: selective
        // Grace joins commonly reject >90% of a fact-table scan, and encoding
        // every rejected composite key was pure per-row CPU.  Check SQL NULL
        // usability explicitly because a non-null-safe NULL must be rejected
        // even if a component Bloom happens to report a false positive.
        let component_prefiltered = component_prefilter_available;
        if component_prefiltered {
            let usable = keys.iter().enumerate().all(|(component, column)| {
                null_safe.get(component).copied().unwrap_or(false)
                    || !batch.columns()[*column].is_null(row_idx)
            });
            if !usable
                || bloom
                    .as_deref()
                    .is_some_and(|filter| !filter.contains(batch, keys, row_idx, &[]))
            {
                filtered = filtered.saturating_add(1);
                continue;
            }
        }
        let usable = vec_hash_join::encode_join_key_with_collations(
            batch,
            keys,
            null_safe,
            key_collations,
            row_idx,
            &mut key,
        );
        if filter_probe && !component_prefiltered {
            // Grace replay previously wrote every probe row before it knew
            // whether any build key could match.  A build-side bloom has no
            // false negatives, so INNER/SEMI joins can discard misses before
            // serialisation while retaining false positives for the exact
            // partition hash table.  Non-null-safe NULL keys are misses too.
            if bloom.as_deref().is_some_and(|filter| {
                !usable || !filter.contains(batch, keys, row_idx, key.as_slice())
            }) {
                filtered = filtered.saturating_add(1);
                continue;
            }
        } else if !filter_probe && usable {
            if let Some(filter) = bloom.as_deref_mut() {
                filter.insert(batch, keys, row_idx, key.as_slice());
            }
        }
        let partition = if usable {
            range
                .and_then(|range| range.partition(&key))
                .unwrap_or_else(|| {
                    crate::engine::executor::select::grace::partition_for_hash(
                        crate::engine::executor::vec::key_encode::fxhash(&key),
                        depth,
                        partitions as u64,
                    ) as usize
                })
        } else {
            // A non-null-safe NULL key cannot match anything. Scatter such
            // rows to keep outer/ANTI preservation balanced without affecting
            // equality semantics.
            let partition = (*null_sequence % partitions as u64) as usize;
            *null_sequence = null_sequence.saturating_add(1);
            partition
        };
        writers.append_batch_row(partition, batch, row_idx)?;
    }
    Ok(filtered)
}

pub(super) fn vec_grace_probe_bloom_safe(join_type: crate::parser::JoinType) -> bool {
    matches!(
        join_type,
        crate::parser::JoinType::Inner | crate::parser::JoinType::Semi
    ) && !matches!(
        crate::util::env::var("MYDB_VEC_GRACE_BLOOM")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "off" | "false" | "no"
    )
}

pub(super) fn push_vec_grace_probe_bloom(
    bloom: &mut Option<GraceProbeBloom>,
    probe: &mut dyn BatchOperator,
    probe_schema: &BatchSchema,
    probe_keys: &[usize],
    null_safe: &[bool],
) -> Vec<std::sync::Arc<std::sync::atomic::AtomicU64>> {
    if matches!(
        crate::util::env::var("MYDB_VEC_GRACE_BLOOM_PUSH")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "off" | "false" | "no"
    ) {
        return Vec::new();
    }
    let Some(candidate) = bloom.take() else {
        return Vec::new();
    };
    let GraceProbeBloom::Components(filters) = candidate else {
        *bloom = Some(candidate);
        return Vec::new();
    };
    let mut retained = Vec::new();
    let mut rejection_counters = Vec::new();
    for (component, filter) in filters {
        let Some(field) = probe_keys
            .get(component)
            .and_then(|index| probe_schema.fields().get(*index))
        else {
            retained.push((component, filter));
            continue;
        };
        let qualifier = field
            .alias
            .as_deref()
            .unwrap_or(&field.table)
            .rsplit('.')
            .next()
            .unwrap_or(&field.table)
            .to_string();
        let keys = vec![crate::parser::Expr::CompoundIdentifier(vec![
            qualifier,
            field.name.clone(),
        ])];
        let filter = std::sync::Arc::new(filter);
        let rejected = analyze_stats::is_recording()
            .then(|| std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)));
        if probe.install_runtime_filter(crate::planner::RuntimeFilter::BloomKey {
            keys,
            null_safe: vec![null_safe.get(component).copied().unwrap_or(false)],
            filter: filter.clone(),
            rejected: rejected.clone(),
        }) {
            if let Some(rejected) = rejected {
                rejection_counters.push(rejected);
            }
        } else {
            let filter = std::sync::Arc::try_unwrap(filter)
                .expect("failed runtime-filter install must not retain the bloom");
            retained.push((component, filter));
        }
    }
    if !retained.is_empty() {
        *bloom = Some(GraceProbeBloom::Components(retained));
    }
    rejection_counters
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_vec_hot_build_chunk(
    rows: &[Vec<Value>],
    build_schema: Arc<BatchSchema>,
    probe_path: &std::path::Path,
    probe_schema: Arc<BatchSchema>,
    charger: crate::engine::executor::spill_quota::SpillCharger,
    build_keys: &[usize],
    probe_keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
    join_type: crate::parser::JoinType,
    spec: Option<&vec_hash_join::VecJoinSpec<'_>>,
    semi_out_schema: Arc<BatchSchema>,
    semi_residual: Option<(&expr_batch::BatchExprProgram, Arc<BatchSchema>)>,
    build_cap: vec_hash_join::BuildCap,
    emit: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    let build_batches = value_row_slice_to_batches(build_schema.clone(), rows);
    let mut build = MaterializedBatchOperator::new(build_schema, build_batches);
    let mut probe = VecJoinSpillBatchStream::open(probe_path, probe_schema, charger)?;
    if matches!(
        join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    ) {
        vec_hash_join::with_vec_build_cap(build_cap, || {
            vec_hash_join::run_semi_anti_hash_join_into(
                &mut build,
                &mut probe,
                build_keys,
                probe_keys,
                key_collations,
                null_safe,
                false,
                semi_out_schema.clone(),
                semi_residual.clone(),
                emit,
            )
        })
    } else {
        vec_hash_join::with_vec_build_cap(build_cap, || {
            vec_hash_join::run_inner_hash_join_into(
                &mut build,
                &mut probe,
                spec.expect("inner/outer hot partition spec"),
                DEFAULT_BATCH_ROWS.min(1024),
                emit,
            )
        })
    }
}

pub(super) fn hot_row_identity(row: &[Value]) -> Result<Vec<u8>, QueryError> {
    let mut key = Vec::new();
    for value in row {
        crate::engine::executor::select::grace::write_value(&mut key, value).map_err(|e| {
            crate::storage::error(crate::error::catalog::ER_UNKNOWN_ERROR, e.to_string())
        })?;
    }
    Ok(key)
}

pub(super) fn insert_hot_match(
    set: &mut std::collections::HashSet<Vec<u8>>,
    row: &[Value],
    charge: &mut mem_budget::ScopedCharge,
) -> Result<(), QueryError> {
    let key = hot_row_identity(row)?;
    if !set.contains(&key) {
        charge.add(key.capacity().saturating_add(64))?;
        set.insert(key);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_vec_hot_partition_chunked(
    build_path: &std::path::Path,
    probe_path: &std::path::Path,
    build_schema: Arc<BatchSchema>,
    probe_schema: Arc<BatchSchema>,
    charger: crate::engine::executor::spill_quota::SpillCharger,
    build_keys: &[usize],
    probe_keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
    join_type: crate::parser::JoinType,
    spec: Option<&vec_hash_join::VecJoinSpec<'_>>,
    semi_out_schema: Arc<BatchSchema>,
    semi_residual: Option<(&expr_batch::BatchExprProgram, Arc<BatchSchema>)>,
    build_cap: vec_hash_join::BuildCap,
    emit: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    use std::collections::HashSet;

    let build_cols_first = spec.is_some_and(|spec| spec.build_cols_first);
    let preserve_build = join_type == crate::parser::JoinType::Full
        || (join_type == crate::parser::JoinType::Left && build_cols_first)
        || (join_type == crate::parser::JoinType::Right && !build_cols_first);
    let preserve_probe = join_type == crate::parser::JoinType::Full
        || (join_type == crate::parser::JoinType::Left && !build_cols_first)
        || (join_type == crate::parser::JoinType::Right && build_cols_first);

    let mut build =
        VecJoinSpillBatchStream::open(build_path, build_schema.clone(), charger.clone())?;
    let max_rows = if build_cap.max_rows == 0 {
        u64::MAX
    } else {
        build_cap.max_rows.max(1)
    };
    let max_bytes = if build_cap.max_bytes == 0 {
        u64::MAX
    } else {
        build_cap.max_bytes.max(1)
    };
    let mut pending = Vec::<Vec<Value>>::new();
    let mut pending_bytes = 0u64;
    let mut ledger_charge = mem_budget::ScopedCharge::new(0);
    let mut matched_probe_rows = HashSet::<Vec<u8>>::new();
    let mut matched_build_rows = HashSet::<Vec<u8>>::new();

    let mut process = |pending: &mut Vec<Vec<Value>>,
                       matched_probe_rows: &mut HashSet<Vec<u8>>,
                       matched_build_rows: &mut HashSet<Vec<u8>>|
     -> Result<(), QueryError> {
        if pending.is_empty() {
            return Ok(());
        }
        let mut ranges = vec![(0usize, pending.len())];
        while let Some((start, end)) = ranges.pop() {
            crate::engine::executor::select::plan::HASH_JOIN_SKEW_CHUNKS
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut consume = |batch: Batch| -> Result<(), QueryError> {
                if matches!(
                    join_type,
                    crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
                ) {
                    for row_idx in batch.visible_indices() {
                        insert_hot_match(
                            matched_probe_rows,
                            &batch.row(row_idx),
                            &mut ledger_charge,
                        )?;
                    }
                    return Ok(());
                }
                if preserve_build || preserve_probe {
                    let build_cols = build_schema.len();
                    let probe_cols = probe_schema.len();
                    for row_idx in batch.visible_indices() {
                        let row = batch.row(row_idx);
                        let (build, probe) = if build_cols_first {
                            (
                                &row[..build_cols],
                                &row[build_cols..build_cols + probe_cols],
                            )
                        } else {
                            (
                                &row[probe_cols..probe_cols + build_cols],
                                &row[..probe_cols],
                            )
                        };
                        if preserve_probe {
                            insert_hot_match(matched_probe_rows, probe, &mut ledger_charge)?;
                        }
                        if preserve_build {
                            insert_hot_match(matched_build_rows, build, &mut ledger_charge)?;
                        }
                    }
                }
                emit(batch)
            };
            let matched = run_vec_hot_build_chunk(
                &pending[start..end],
                build_schema.clone(),
                probe_path,
                probe_schema.clone(),
                charger.clone(),
                build_keys,
                probe_keys,
                null_safe,
                key_collations,
                join_type,
                spec,
                semi_out_schema.clone(),
                semi_residual.clone(),
                build_cap,
                &mut consume,
            );
            match matched {
                Ok(()) => (),
                Err(error) if vec_hash_join::is_vec_build_overflow(&error) && end - start > 1 => {
                    // The serialized-row estimate cannot predict arbitrary
                    // encoded collation keys or a Generic-lane promotion.
                    // Bisect only this spilled build chunk and replay the
                    // probe; every successful child stays within the exact
                    // cap and no physical input is rescanned.
                    let middle = start + (end - start) / 2;
                    ranges.push((middle, end));
                    ranges.push((start, middle));
                    continue;
                }
                Err(error) if vec_hash_join::is_vec_build_overflow(&error) => {
                    return Err(crate::engine::executor::mem_budget::allocation_error(
                        "vector hash join build row exceeds its work area",
                    ));
                }
                Err(error) => return Err(error),
            };
        }
        pending.clear();
        Ok(())
    };

    while let Some(batch) = build.next_batch()? {
        for row_idx in batch.visible_indices() {
            crate::engine::cancel::check_cancelled()?;
            let row = batch.row(row_idx);
            let row_bytes = crate::engine::executor::select::grace::estimate_value_row_bytes(&row)
                .saturating_add(vec_hash_join::estimate_build_index_bytes(1));
            if !pending.is_empty()
                && (pending.len() as u64 >= max_rows
                    || pending_bytes.saturating_add(row_bytes) > max_bytes)
            {
                process(
                    &mut pending,
                    &mut matched_probe_rows,
                    &mut matched_build_rows,
                )?;
                pending_bytes = 0;
            }
            pending_bytes = pending_bytes.saturating_add(row_bytes);
            pending.push(row);
        }
    }
    process(
        &mut pending,
        &mut matched_probe_rows,
        &mut matched_build_rows,
    )?;

    if matches!(join_type, crate::parser::JoinType::Inner) {
        return Ok(());
    }
    let final_schema = match join_type {
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti => semi_out_schema.clone(),
        _ => spec.expect("outer hot partition spec").out_schema.clone(),
    };
    let mut final_rows = Vec::new();
    let mut probe =
        VecJoinSpillBatchStream::open(probe_path, probe_schema.clone(), charger.clone())?;
    while let Some(batch) = probe.next_batch()? {
        for row_idx in batch.visible_indices() {
            crate::engine::cancel::check_cancelled()?;
            let probe_row = batch.row(row_idx);
            let matched = matched_probe_rows.contains(&hot_row_identity(&probe_row)?);
            match join_type {
                crate::parser::JoinType::Semi if matched => final_rows.push(probe_row),
                crate::parser::JoinType::Anti if !matched => final_rows.push(probe_row),
                crate::parser::JoinType::Left
                | crate::parser::JoinType::Right
                | crate::parser::JoinType::Full
                    if preserve_probe && !matched =>
                {
                    let join_spec = spec.expect("outer hot partition spec");
                    let null_build = vec![Value::Null; build_schema.fields().len()];
                    let mut row = Vec::with_capacity(
                        build_schema.fields().len() + probe_schema.fields().len(),
                    );
                    if join_spec.build_cols_first {
                        row.extend(null_build);
                        row.extend(probe_row);
                    } else {
                        row.extend(probe_row);
                        row.extend(null_build);
                    }
                    final_rows.push(row);
                }
                _ => {}
            }
            if final_rows.len() >= 1024 {
                for batch in
                    value_rows_to_batches(final_schema.clone(), std::mem::take(&mut final_rows))
                {
                    emit(batch)?;
                }
            }
        }
    }
    if preserve_build {
        let join_spec = spec.expect("build-preserved hot partition spec");
        let null_probe = vec![Value::Null; probe_schema.fields().len()];
        let mut build = VecJoinSpillBatchStream::open(build_path, build_schema.clone(), charger)?;
        while let Some(batch) = build.next_batch()? {
            for row_idx in batch.visible_indices() {
                let build_row = batch.row(row_idx);
                if matched_build_rows.contains(&hot_row_identity(&build_row)?) {
                    continue;
                }
                let mut row =
                    Vec::with_capacity(build_schema.fields().len() + probe_schema.fields().len());
                if join_spec.build_cols_first {
                    row.extend(build_row);
                    row.extend(null_probe.iter().cloned());
                } else {
                    row.extend(null_probe.iter().cloned());
                    row.extend(build_row);
                }
                final_rows.push(row);
                if final_rows.len() >= 1024 {
                    for batch in
                        value_rows_to_batches(final_schema.clone(), std::mem::take(&mut final_rows))
                    {
                        emit(batch)?;
                    }
                }
            }
        }
    }
    for batch in value_rows_to_batches(final_schema, final_rows) {
        emit(batch)?;
    }
    Ok(())
}

/// A Grace result retains bounded batches or quota-owned files, never the
/// complete unaccounted output of all replay partitions. Readers open lazily
/// so partition count does not multiply resident I/O buffers.
pub(super) struct GraceJoinOutput {
    pub(super) schema: Arc<BatchSchema>,
    pub(super) parts: std::collections::VecDeque<GraceJoinPart>,
}

pub(super) enum GraceJoinPart {
    #[cfg(test)]
    Memory {
        batches: std::vec::IntoIter<Batch>,
        _charge: Option<mem_budget::OwnedMemCharge>,
    },
    Disk {
        dir: crate::engine::executor::select::grace::SpillTmpDir,
        reader: Option<crate::engine::executor::spill_quota::TrackedSpillReader>,
    },
}

impl GraceJoinOutput {
    pub(super) fn empty(schema: Arc<BatchSchema>) -> Self {
        Self {
            schema,
            parts: Default::default(),
        }
    }

    #[cfg(test)]
    pub(super) fn store(
        db: &Arc<Database>,
        schema: Arc<BatchSchema>,
        batches: Vec<Batch>,
    ) -> Result<Self, QueryError> {
        let mut output = Self::empty(schema);
        if batches.is_empty() {
            return Ok(output);
        }
        let bytes = batches.iter().fold(
            batches.capacity() * std::mem::size_of::<Batch>(),
            |total, batch| total.saturating_add(batch.memory_bytes()),
        );
        let budget = mem_budget::current_budget();
        let charge = budget.as_ref().and_then(|budget| {
            (bytes <= budget.limit() / 8
                && budget.used().saturating_add(bytes) <= budget.limit() / 4)
                .then(|| mem_budget::OwnedMemCharge::new(budget.clone(), bytes).ok())
                .flatten()
        });
        if charge.is_some() || budget.is_none() {
            output.parts.push_back(GraceJoinPart::Memory {
                batches: batches.into_iter(),
                _charge: charge,
            });
            return Ok(output);
        }
        let dir = crate::engine::executor::select::grace::SpillTmpDir::create_kind(
            db.data_dir(),
            "vec-join-output",
        )?;
        let mut writer = crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
            &dir.path().join("output"),
            dir.charger(),
            32 * 1024,
        )?;
        for batch in batches {
            crate::engine::cancel::check_cancelled()?;
            if batch.selection().is_some() {
                let indices = batch
                    .visible_indices()
                    .map(|index| index as u32)
                    .collect::<Vec<_>>();
                let compact = Batch::with_row_count(
                    batch.schema().clone(),
                    batch
                        .columns()
                        .iter()
                        .map(|column| column.take_indices(&indices))
                        .collect(),
                    indices.len(),
                );
                crate::engine::executor::vec::spill::write_batch(&mut writer, &compact)?;
            } else {
                crate::engine::executor::vec::spill::write_batch(&mut writer, &batch)?;
            }
        }
        let bytes = writer.finish()?;
        crate::engine::executor::spill_hint::record_written(bytes);
        output
            .parts
            .push_back(GraceJoinPart::Disk { dir, reader: None });
        Ok(output)
    }

    pub(super) fn write_from(
        db: &Arc<Database>,
        schema: Arc<BatchSchema>,
        produce: impl FnOnce(&mut dyn FnMut(Batch) -> Result<(), QueryError>) -> Result<(), QueryError>,
    ) -> Result<Self, QueryError> {
        let dir = crate::engine::executor::select::grace::SpillTmpDir::create_kind(
            db.data_dir(),
            "vec-join-output",
        )?;
        let mut writer = crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
            &dir.path().join("output"),
            dir.charger(),
            32 * 1024,
        )?;
        produce(&mut |batch| {
            let _charge = mem_budget::force_charge_scoped(batch.memory_bytes())?;
            if batch.selection().is_some() {
                let indices = batch
                    .visible_indices()
                    .map(|row| row as u32)
                    .collect::<Vec<_>>();
                let compact = Batch::with_row_count(
                    batch.schema().clone(),
                    batch
                        .columns()
                        .iter()
                        .map(|column| column.take_indices(&indices))
                        .collect(),
                    indices.len(),
                );
                crate::engine::executor::vec::spill::write_batch(&mut writer, &compact)
            } else {
                crate::engine::executor::vec::spill::write_batch(&mut writer, &batch)
            }
        })?;
        let bytes = writer.finish()?;
        crate::engine::executor::spill_hint::record_written(bytes);
        let mut output = Self::empty(schema);
        output
            .parts
            .push_back(GraceJoinPart::Disk { dir, reader: None });
        Ok(output)
    }

    pub(super) fn append(&mut self, mut other: Self) {
        self.parts.append(&mut other.parts);
    }

    #[cfg(test)]
    pub(super) fn collect(mut self) -> Result<Vec<Batch>, QueryError> {
        let mut batches = Vec::new();
        let mut charge = mem_budget::ScopedCharge::new(0);
        while let Some(batch) = self.next_batch()? {
            charge.add(batch.memory_bytes())?;
            batches.push(batch);
        }
        crate::engine::executor::select::plan::commit_transient_materialization_charge(charge);
        Ok(batches)
    }
}

impl BatchOperator for GraceJoinOutput {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        loop {
            crate::engine::cancel::check_cancelled()?;
            let Some(part) = self.parts.front_mut() else {
                return Ok(None);
            };
            let batch = match part {
                #[cfg(test)]
                GraceJoinPart::Memory { batches, .. } => batches.next(),
                GraceJoinPart::Disk { dir, reader } => {
                    if reader.is_none() {
                        *reader = Some(
                            crate::engine::executor::spill_quota::TrackedSpillReader::open(
                                &dir.path().join("output"),
                                dir.charger(),
                                32 * 1024,
                            )?,
                        );
                    }
                    crate::engine::executor::vec::spill::read_owned_batch(
                        reader.as_mut().unwrap(),
                        self.schema.clone(),
                    )?
                }
            };
            if batch.is_some() {
                return Ok(batch);
            }
            self.parts.pop_front();
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_vec_grace_partition(
    db: &Arc<Database>,
    txn: TxnView,
    tmp_path: &std::path::Path,
    charger: crate::engine::executor::spill_quota::SpillCharger,
    partition: usize,
    build_schema: Arc<BatchSchema>,
    probe_schema: Arc<BatchSchema>,
    build_keys: &[usize],
    probe_keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
    join_type: crate::parser::JoinType,
    spec: Option<&vec_hash_join::VecJoinSpec<'_>>,
    semi_out_schema: Arc<BatchSchema>,
    semi_residual: Option<(&expr_batch::BatchExprProgram, Arc<BatchSchema>)>,
    partitions: usize,
    build_cap: vec_hash_join::BuildCap,
    max_depth: u32,
    depth: u32,
) -> Result<GraceJoinOutput, QueryError> {
    let output_schema = match join_type {
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti => semi_out_schema.clone(),
        _ => spec
            .map(|spec| spec.out_schema.clone())
            .unwrap_or_else(|| semi_out_schema.clone()),
    };
    let build_path = tmp_path.join(format!("build-{partition}"));
    let probe_path = tmp_path.join(format!("probe-{partition}"));
    let mut build =
        VecJoinSpillBatchStream::open(&build_path, build_schema.clone(), charger.clone())?;
    let mut probe =
        VecJoinSpillBatchStream::open(&probe_path, probe_schema.clone(), charger.clone())?;
    let (build_prefix, overflow, build_memory_charge) = drain_vec_join_build_to_cap_impl(
        &mut build,
        build_cap,
        None,
        mem_budget::current_budget()
            .map(|budget| {
                (budget.limit() / scheduler_hash_join_workers().max(1) / 8).max(64 * 1024)
            })
            .unwrap_or(0),
    )?;
    if overflow {
        crate::engine::executor::select::plan::HASH_JOIN_GRACE_REPARTITIONS
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if depth >= max_depth {
            // The hot-partition fallback reopens both files from their start.
            // Release the failed preflight and its readers before replay.
            drop(build_prefix);
            drop(build_memory_charge);
            drop(build);
            drop(probe);
            return GraceJoinOutput::write_from(db, output_schema, |emit| {
                run_vec_hot_partition_chunked(
                    &build_path,
                    &probe_path,
                    build_schema,
                    probe_schema,
                    charger,
                    build_keys,
                    probe_keys,
                    null_safe,
                    key_collations,
                    join_type,
                    spec,
                    semi_out_schema,
                    semi_residual,
                    build_cap,
                    emit,
                )
            });
        }
        return run_vec_grace_hash_join_stream(
            db,
            txn,
            build_prefix,
            Some(build_memory_charge),
            &mut build,
            &mut probe,
            build_keys,
            probe_keys,
            null_safe,
            key_collations,
            join_type,
            spec,
            semi_out_schema,
            semi_residual,
            partitions,
            build_cap,
            max_depth,
            depth + 1,
            false,
        );
    }

    let mut build = MaterializedBatchOperator::new(build_schema.clone(), build_prefix);
    let batch_rows = probe.batch_rows;
    let result = vec_hash_join::with_vec_build_cap(build_cap, || {
        GraceJoinOutput::write_from(db, output_schema.clone(), |emit| match join_type {
            crate::parser::JoinType::Inner => vec_hash_join::run_inner_hash_join_into(
                &mut build,
                &mut probe,
                spec.expect("inner grace spec"),
                batch_rows,
                emit,
            ),
            crate::parser::JoinType::Left
            | crate::parser::JoinType::Right
            | crate::parser::JoinType::Full => {
                let spec = spec.expect("outer grace spec");
                let preserve_build = join_type == crate::parser::JoinType::Full
                    || (join_type == crate::parser::JoinType::Left && spec.build_cols_first)
                    || (join_type == crate::parser::JoinType::Right && !spec.build_cols_first);
                let preserve_probe = join_type == crate::parser::JoinType::Full || !preserve_build;
                vec_hash_join::run_outer_hash_join_into(
                    &mut build,
                    &mut probe,
                    spec,
                    preserve_probe,
                    preserve_build,
                    batch_rows,
                    emit,
                )
            }
            crate::parser::JoinType::Semi | crate::parser::JoinType::Anti => {
                vec_hash_join::run_semi_anti_hash_join_into(
                    &mut build,
                    &mut probe,
                    build_keys,
                    probe_keys,
                    key_collations,
                    null_safe,
                    join_type == crate::parser::JoinType::Anti,
                    semi_out_schema.clone(),
                    semi_residual.clone(),
                    emit,
                )
            }
            _ => unreachable!("preflighted vec grace join type"),
        })
    });
    match result {
        Ok(output) => Ok(output),
        Err(error) if vec_hash_join::is_vec_build_overflow(&error) => {
            // `drain_vec_join_build_to_cap` deliberately uses a cheap
            // conservative estimate, while the collector knows the encoded
            // key sizes and the final hash layout.  A long/composite key can
            // therefore pass preflight and still cross the exact cap.  The
            // spill files are the replay source of truth, so reopen them and
            // handle this identically to a preflight overflow; the internal
            // sentinel must never escape to SQL clients.
            crate::engine::executor::select::plan::HASH_JOIN_GRACE_REPARTITIONS
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Exact-key overflow restarts from disk. No failed build or
            // reservation may remain live throughout the recursive replay.
            drop(build);
            drop(build_memory_charge);
            drop(probe);
            if depth >= max_depth {
                return GraceJoinOutput::write_from(db, output_schema, |emit| {
                    run_vec_hot_partition_chunked(
                        &build_path,
                        &probe_path,
                        build_schema,
                        probe_schema,
                        charger,
                        build_keys,
                        probe_keys,
                        null_safe,
                        key_collations,
                        join_type,
                        spec,
                        semi_out_schema,
                        semi_residual,
                        build_cap,
                        emit,
                    )
                });
            }
            let mut build =
                VecJoinSpillBatchStream::open(&build_path, build_schema, charger.clone())?;
            let mut probe = VecJoinSpillBatchStream::open(&probe_path, probe_schema, charger)?;
            run_vec_grace_hash_join_stream(
                db,
                txn,
                Vec::new(),
                None,
                &mut build,
                &mut probe,
                build_keys,
                probe_keys,
                null_safe,
                key_collations,
                join_type,
                spec,
                semi_out_schema,
                semi_residual,
                partitions,
                build_cap,
                max_depth,
                depth + 1,
                false,
            )
        }
        Err(error) => Err(error),
    }
}

#[cfg(not(test))]
pub(super) const VEC_GRACE_PARALLEL_MIN_ROWS: u64 = 100_000;
// Keep SQL regressions small while still exercising the identical scheduling,
// reservation and ordered-merge implementation.
#[cfg(test)]
pub(super) const VEC_GRACE_PARALLEL_MIN_ROWS: u64 = 128;

pub(super) fn vec_grace_worker_cap(
    cap: vec_hash_join::BuildCap,
    workers: usize,
) -> Option<vec_hash_join::BuildCap> {
    if workers <= 1 || cap.max_bytes < (workers as u64).saturating_mul(64 * 1024) {
        return None;
    }
    Some(vec_hash_join::BuildCap {
        max_rows: if cap.max_rows == 0 {
            0
        } else {
            (cap.max_rows / workers as u64).max(1)
        },
        max_bytes: cap.max_bytes / workers as u64,
    })
}

pub(super) fn vec_grace_partition_count(
    configured_max: usize,
    build_prefix: &[Batch],
    estimated_build_rows: f64,
    build_cap: vec_hash_join::BuildCap,
    workers: usize,
) -> usize {
    let configured_max = configured_max.clamp(2, 256);
    let baseline = configured_max.min(16);
    let observed_rows = build_prefix.iter().map(Batch::visible_rows).sum::<usize>() as u64;
    if observed_rows == 0 || build_cap.max_bytes == 0 || workers == 0 {
        return baseline;
    }
    let observed_bytes = build_prefix.iter().fold(0u64, |bytes, batch| {
        let rows = batch.visible_rows() as u64;
        bytes
            .saturating_add(batch.memory_bytes() as u64)
            .saturating_add(vec_hash_join::estimate_build_index_bytes(rows))
    });
    let bytes_per_row = observed_bytes.div_ceil(observed_rows).max(1);
    let estimated_rows = if estimated_build_rows.is_finite() && estimated_build_rows > 0.0 {
        estimated_build_rows.ceil().min(u64::MAX as f64) as u64
    } else {
        observed_rows
    }
    .max(observed_rows);
    let estimated_bytes = bytes_per_row.saturating_mul(estimated_rows);
    let replay_cap = (build_cap.max_bytes / workers as u64).max(64 * 1024);
    // Leave headroom for batch-capacity rounding and hash-table skew. This is
    // a stable open-time layout decision from plan cardinality + the already
    // consumed cap-crossing prefix; live RSS/cache pressure is not consulted.
    let target_partition_bytes = replay_cap.saturating_mul(3) / 4;
    let needed = estimated_bytes
        .div_ceil(target_partition_bytes.max(1))
        .clamp(2, 256) as usize;
    needed.next_power_of_two().max(baseline).min(configured_max)
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(super) fn run_vec_grace_hash_join(
    db: &Arc<Database>,
    txn: TxnView,
    build_prefix: Vec<Batch>,
    build_prefix_charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
    build_remaining: &mut dyn BatchOperator,
    probe: &mut dyn BatchOperator,
    build_keys: &[usize],
    probe_keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
    join_type: crate::parser::JoinType,
    spec: Option<&vec_hash_join::VecJoinSpec<'_>>,
    semi_out_schema: Arc<BatchSchema>,
    semi_residual: Option<(&expr_batch::BatchExprProgram, Arc<BatchSchema>)>,
    partitions: usize,
    build_cap: vec_hash_join::BuildCap,
    max_depth: u32,
    depth: u32,
) -> Result<Vec<Batch>, QueryError> {
    run_vec_grace_hash_join_stream(
        db,
        txn,
        build_prefix,
        build_prefix_charge,
        build_remaining,
        probe,
        build_keys,
        probe_keys,
        null_safe,
        key_collations,
        join_type,
        spec,
        semi_out_schema,
        semi_residual,
        partitions,
        build_cap,
        max_depth,
        depth,
        false,
    )?
    .collect()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_vec_grace_hash_join_stream(
    db: &Arc<Database>,
    txn: TxnView,
    build_prefix: Vec<Batch>,
    build_prefix_charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
    build_remaining: &mut dyn BatchOperator,
    probe: &mut dyn BatchOperator,
    build_keys: &[usize],
    probe_keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
    join_type: crate::parser::JoinType,
    spec: Option<&vec_hash_join::VecJoinSpec<'_>>,
    semi_out_schema: Arc<BatchSchema>,
    semi_residual: Option<(&expr_batch::BatchExprProgram, Arc<BatchSchema>)>,
    partitions: usize,
    build_cap: vec_hash_join::BuildCap,
    max_depth: u32,
    depth: u32,
    prefer_key_locality: bool,
) -> Result<GraceJoinOutput, QueryError> {
    crate::engine::executor::select::plan::HASH_JOIN_GRACE_MAX_DEPTH
        .fetch_max(depth as u64, std::sync::atomic::Ordering::Relaxed);
    // Grace partitions rows without constructing the resident hash index.
    // Retain the crossing batches' capacity charge, but relinquish the unused
    // index allowance before admitting optional filters and writer buffers.
    // Collector-overflow callers have already dropped the failed index.
    let mut build_prefix_charge = build_prefix_charge;
    if let Some(charge) = build_prefix_charge.as_mut() {
        let retained_bytes = build_prefix.iter().fold(
            build_prefix
                .capacity()
                .saturating_mul(std::mem::size_of::<Batch>()),
            |bytes, batch| bytes.saturating_add(batch.memory_bytes()),
        );
        charge.release_bytes(charge.charged().saturating_sub(retained_bytes));
    }
    let partitions = partitions.max(2);
    let build_schema = build_remaining.schema().clone();
    let probe_schema = probe.schema().clone();
    let tmp = crate::engine::executor::select::grace::SpillTmpDir::create_kind(
        db.data_dir(),
        "vec-hashjoin",
    )?;
    let mut build_writers =
        crate::engine::executor::select::grace::LazyValueRowWriterPool::new_with_concurrent_pools(
            tmp.path(),
            "build",
            partitions,
            build_schema.fields().len(),
            tmp.charger(),
            partitions.min(256),
            1,
        );

    // Size from the already materialised crossing prefix.  It is a stable
    // lower bound available without rescanning or consulting live memory.
    // Capping the estimate bounds this optional filter at 16 MiB (the Bloom
    // layout is a power of two); a larger actual build only raises the false
    // positive rate and can never change results. The reservation is attached
    // to the bitmap and follows every pushed runtime-filter Arc until its last
    // owner releases it.
    let prefix_rows = build_prefix
        .iter()
        .map(|batch| batch.visible_rows())
        .sum::<usize>();
    let query_budget = mem_budget::current_budget();
    let use_components = !build_keys.is_empty()
        && build_keys.len() <= 3
        && key_collations.iter().all(Option::is_none);
    let component_count = if use_components { build_keys.len() } else { 1 };
    // Divide the fixed optional-filter budget across key components. The
    // total remains bounded while independent component filters allow scan
    // evaluation to short-circuit before constructing a composite key.
    let bloom_entries = prefix_rows
        .saturating_mul(2)
        .div_ceil(component_count)
        .clamp(1_024, 8_000_000);
    let bloom_bytes =
        crate::engine::executor::select::bloom::BloomFilter::estimated_byte_size(bloom_entries)
            .saturating_mul(component_count);
    let mut grace_bloom = if vec_grace_probe_bloom_safe(join_type) {
        query_budget.as_ref().and_then(|budget| {
            crate::engine::executor::mem_budget::OwnedMemCharge::new_optional_pipeline(
                budget.clone(),
                bloom_bytes,
            )
            .ok()
            .map(|charge| {
                let charge = std::sync::Arc::new(charge);
                let int_component = |component: usize| {
                    !null_safe.get(component).copied().unwrap_or(false)
                        && vec_hash_join::schema_column_uses_i64_lane(
                            &build_schema,
                            build_keys[component],
                        )
                };
                let mut filter = if use_components && int_component(0) {
                    crate::engine::executor::select::bloom::BloomFilter::new_int(bloom_entries)
                } else {
                    crate::engine::executor::select::bloom::BloomFilter::new(bloom_entries)
                };
                filter.attach_charge(charge.clone());
                if use_components {
                    let mut filters = Vec::with_capacity(component_count);
                    filters.push((0, filter));
                    filters.extend((1..component_count).map(|component| {
                        let mut filter = if int_component(component) {
                            crate::engine::executor::select::bloom::BloomFilter::new_int(
                                bloom_entries,
                            )
                        } else {
                            crate::engine::executor::select::bloom::BloomFilter::new(bloom_entries)
                        };
                        filter.attach_charge(charge.clone());
                        (component, filter)
                    }));
                    GraceProbeBloom::Components(filters)
                } else {
                    GraceProbeBloom::Encoded(filter)
                }
            })
        })
    } else {
        None
    };
    if grace_bloom.is_some() {
        crate::engine::executor::select::plan::record_hash_join_bloom_built();
    }

    let range = prefer_key_locality
        .then(|| {
            GraceRangePartitioning::from_prefix(
                &build_prefix,
                build_keys,
                probe_keys,
                null_safe,
                key_collations,
                partitions,
                depth,
            )
        })
        .flatten();
    if range.is_some() {
        VEC_HASH_JOIN_RANGE_PARTITION_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let mut build_null_sequence = 0u64;
    let mut build_rows = 0u64;
    let mut probe_rows = 0u64;
    for batch in &build_prefix {
        build_rows = build_rows.saturating_add(batch.visible_rows() as u64);
        spill_vec_join_batch(
            batch,
            build_keys,
            null_safe,
            key_collations,
            &mut build_writers,
            &mut build_null_sequence,
            depth,
            range.as_ref(),
            grace_bloom.as_mut(),
            false,
        )?;
    }
    // The crossing prefix has been persisted and is no longer an input to
    // replay. Keeping it alive until every partition completed retained a
    // large duplicate of the build side and reduced the safe parallel replay
    // grant for no semantic benefit.
    drop(build_prefix);
    drop(build_prefix_charge);
    while let Some(batch) = build_remaining.next_batch()? {
        let batch_bytes = batch.memory_bytes().saturating_add(
            vec_hash_join::estimate_build_index_bytes(batch.visible_rows() as u64)
                .min(usize::MAX as u64) as usize,
        );
        let _batch_charge = mem_budget::force_charge_scoped(batch_bytes)?;
        build_rows = build_rows.saturating_add(batch.visible_rows() as u64);
        spill_vec_join_batch(
            &batch,
            build_keys,
            null_safe,
            key_collations,
            &mut build_writers,
            &mut build_null_sequence,
            depth,
            range.as_ref(),
            grace_bloom.as_mut(),
            false,
        )?;
    }
    // Build and probe partitioning are strictly phase-serial. Close the build
    // descriptors and release their governed buffers before opening probe
    // writers; keeping both pools alive halved every writer buffer and doubled
    // the live descriptor set without enabling any overlap.
    let build_spill_bytes = build_writers.finish()?;
    let mut probe_writers =
        crate::engine::executor::select::grace::LazyValueRowWriterPool::new_with_concurrent_pools(
            tmp.path(),
            "probe",
            partitions,
            probe_schema.fields().len(),
            tmp.charger(),
            partitions.min(256),
            1,
        );
    let pushed_bloom_rejected = push_vec_grace_probe_bloom(
        &mut grace_bloom,
        probe,
        &probe_schema,
        probe_keys,
        null_safe,
    );
    let mut probe_null_sequence = 0u64;
    let mut bloom_filtered = 0u64;
    while let Some(batch) = probe.next_batch()? {
        probe_rows = probe_rows.saturating_add(batch.visible_rows() as u64);
        bloom_filtered = bloom_filtered.saturating_add(spill_vec_join_batch(
            &batch,
            probe_keys,
            null_safe,
            key_collations,
            &mut probe_writers,
            &mut probe_null_sequence,
            depth,
            range.as_ref(),
            grace_bloom.as_mut(),
            true,
        )?);
    }
    let spill_bytes = build_spill_bytes.saturating_add(probe_writers.finish()?);
    VEC_HASH_JOIN_SPILL_BYTES.fetch_add(spill_bytes, std::sync::atomic::Ordering::Relaxed);
    analyze_stats::add_pending_hash_join_spill(spill_bytes, partitions as u64);
    if bloom_filtered > 0 {
        crate::engine::executor::select::plan::record_hash_join_bloom_filtered_n(bloom_filtered);
        analyze_stats::add_pending_runtime_filter_rejected(bloom_filtered);
    }
    for rejected in pushed_bloom_rejected {
        analyze_stats::add_pending_runtime_filter_rejected(
            rejected.load(std::sync::atomic::Ordering::Relaxed),
        );
    }
    drop(grace_bloom);
    if depth == 0 {
        analyze_stats::add_pending_join_runtime(build_rows, probe_rows, 0);
    }

    let requested_workers =
        if depth == 0 && build_rows.saturating_add(probe_rows) >= VEC_GRACE_PARALLEL_MIN_ROWS {
            scheduler_hash_join_workers().min(partitions)
        } else {
            1
        };
    let parallel =
        vec_grace_worker_cap(build_cap, requested_workers).filter(|_| query_budget.is_some());
    // Each replay worker charges its actual resident batches and build index
    // in drain_vec_join_build_to_cap. Reserving the whole cap here as well
    // double-counts that same working set and can make every partition spill
    // recursively despite fitting its worker cap. Dividing the cap bounds the
    // aggregate build size; the shared governor still checks each build charge.
    let (workers, replay_cap) = match parallel {
        Some(worker_cap) => (requested_workers, worker_cap),
        None => (1, build_cap),
    };

    let output_schema = match join_type {
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti => semi_out_schema.clone(),
        _ => spec
            .map(|spec| spec.out_schema.clone())
            .unwrap_or_else(|| semi_out_schema.clone()),
    };
    let mut output = GraceJoinOutput::empty(output_schema);
    if workers == 1 {
        for partition in 0..partitions {
            let batches = run_vec_grace_partition(
                db,
                txn,
                tmp.path(),
                tmp.charger(),
                partition,
                build_schema.clone(),
                probe_schema.clone(),
                build_keys,
                probe_keys,
                null_safe,
                key_collations,
                join_type,
                spec,
                semi_out_schema.clone(),
                semi_residual.clone(),
                partitions,
                replay_cap,
                max_depth,
                depth,
            )?;
            output.append(batches);
        }
    } else {
        VEC_HASH_JOIN_GRACE_PARALLEL_WORKERS
            .fetch_add(workers as u64, std::sync::atomic::Ordering::Relaxed);
        crate::engine::executor::select::plan::record_hash_join_parallel_partitions(workers as u64);
        let cursor = std::sync::atomic::AtomicUsize::new(0);
        let execution_context =
            crate::engine::executor::select::execution_context::ExecutionContext::capture(txn);
        let mut worker_results: Vec<Result<Vec<(usize, GraceJoinOutput)>, QueryError>> =
            (0..workers).map(|_| Ok(Vec::new())).collect();
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers);
            for _ in 0..workers {
                let worker_context = execution_context.clone();
                let cursor = &cursor;
                let charger = tmp.charger();
                let tmp_path = tmp.path().to_path_buf();
                let build_schema = build_schema.clone();
                let probe_schema = probe_schema.clone();
                let semi_out_schema = semi_out_schema.clone();
                let semi_residual = semi_residual.clone();
                handles.push(scope.spawn(
                    move || -> Result<Vec<(usize, GraceJoinOutput)>, QueryError> {
                        let run = || -> Result<Vec<(usize, GraceJoinOutput)>, QueryError> {
                            let mut local = Vec::new();
                            loop {
                                let partition =
                                    cursor.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                if partition >= partitions {
                                    break;
                                }
                                let batches = run_vec_grace_partition(
                                    db,
                                    txn,
                                    &tmp_path,
                                    charger.clone(),
                                    partition,
                                    build_schema.clone(),
                                    probe_schema.clone(),
                                    build_keys,
                                    probe_keys,
                                    null_safe,
                                    key_collations,
                                    join_type,
                                    spec,
                                    semi_out_schema.clone(),
                                    semi_residual.clone(),
                                    partitions,
                                    replay_cap,
                                    max_depth,
                                    depth,
                                )?;
                                local.push((partition, batches));
                            }
                            Ok(local)
                        };
                        worker_context.run(db, run)
                    },
                ));
            }
            for (worker, handle) in handles.into_iter().enumerate() {
                worker_results[worker] = match handle.join() {
                    Ok(result) => result,
                    Err(_) => Err(crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "parallel Grace hash join worker panicked",
                    )),
                };
            }
        });
        let mut partition_results: Vec<Option<GraceJoinOutput>> =
            (0..partitions).map(|_| None).collect();
        for result in worker_results {
            for (partition, batches) in result? {
                if partition >= partitions
                    || partition_results[partition].replace(batches).is_some()
                {
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "parallel Grace hash join returned a duplicate partition",
                    ));
                }
            }
        }
        for batches in partition_results {
            let Some(batches) = batches else {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "parallel Grace hash join did not return every partition",
                ));
            };
            output.append(batches);
        }
    }
    Ok(output)
}

pub(crate) static VEC_HASH_JOIN_SPILL_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static VEC_HASH_JOIN_RANGE_PARTITION_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static VEC_HASH_JOIN_GRACE_PARALLEL_WORKERS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
