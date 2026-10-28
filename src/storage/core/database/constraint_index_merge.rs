// A raw index walk is not an MVCC read. These results are speculative until
// the caller drains writers and certifies the untouched capture interval.
fn constraint_capture_is_empty(jobs: &[Arc<AlterCopyJob>]) -> Result<bool, QueryError> {
    for job in jobs {
        Database::check_alter_copy_cancelled(job)?;
        let log = &job.changes;
        if log.invalidated.load(Ordering::Acquire) {
            return Err(error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "online constraint capture was invalidated",
            ));
        }
        if log.len() != 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

struct ConstraintIndexCursor<'a> {
    public: Option<IndexRangeCursor>,
    building: Option<&'a IndexFile>,
    resume: Option<(IndexKey, RowId)>,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
}
impl ConstraintIndexCursor<'_> {
    fn next(&mut self) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        const CHUNK: usize = 128;
        if let Some(cursor) = &mut self.public {
            return cursor.next_chunk(CHUNK);
        }
        let entries = self.building.unwrap().range_entries_chunk(
            self.low.as_ref(),
            self.high.as_ref(),
            self.resume.as_ref(),
            CHUNK,
        )?;
        self.resume = entries.last().cloned();
        Ok(entries)
    }
}

impl Database {
    #[cfg(test)]
    pub(crate) fn last_constraint_used_index_merge_for_test(&self) -> bool {
        CONSTRAINT_LAST_INDEX_MERGE.with(|used| used.get())
    }

    fn constraint_merge_has_repairs(
        &self,
        validation: &OnlineConstraintValidation,
        targets: &[OnlineIndexTarget],
    ) -> bool {
        validation.fks.iter().any(|state| {
            let child_index = validation
                .child
                .indexes
                .iter()
                .find(|index| index.index_id == state.fk.backing_index_id)
                .or_else(|| {
                    targets
                        .iter()
                        .find(|target| target.index.index_id == state.fk.backing_index_id)
                        .map(|target| &target.index)
                });
            child_index.is_none_or(|index| {
                self.index_repair_state
                    .has_entries_for(validation.child.id, &index.name)
            }) || self
                .index_repair_state
                .has_entries_for(state.parent.id, &state.index.name)
        })
    }

    fn try_constraint_index_merge(
        &self,
        validation: &mut OnlineConstraintValidation,
        targets: &[OnlineIndexTarget],
        jobs: &[Arc<AlterCopyJob>],
    ) -> Result<Option<Option<usize>>, QueryError> {
        let child = &validation.child;
        if !validation.checks.is_empty()
            || validation.fks.is_empty()
            || child.is_partitioned()
            || !constraint_capture_is_empty(jobs)?
        {
            return Ok(None);
        }
        let mut indexes = Vec::new();
        for state in &validation.fks {
            let Some(index) = child
                .indexes
                .iter()
                .find(|i| i.index_id == state.fk.backing_index_id)
                .or_else(|| {
                    targets
                        .iter()
                        .find(|t| t.index.index_id == state.fk.backing_index_id)
                        .map(|t| &t.index)
                })
            else {
                return Ok(None);
            };
            let arity = state.fk.columns.len();
            let eligible = |index: &IndexSchema| {
                !index.is_fulltext()
                    && !index.is_spatial()
                    && !index.is_vector()
                    && !index.has_expression_parts()
                    && !index.has_multi_valued_parts()
                    && (0..arity).all(|pos| {
                        index.column_prefix_len(pos).is_none()
                            && index.column_direction(pos)
                                == super::schema::IndexColumnDirection::Asc
                    })
            };
            if state.parent.is_partitioned() || !eligible(index) || !eligible(&state.index) {
                return Ok(None);
            }
            for (&c, &p) in state.fk.child_col_idx.iter().zip(&state.parent_columns) {
                let c = &child.columns[c];
                let p = &state.parent.columns[p];
                // FK compatibility is weaker than identical key encoding
                // (e.g. DECIMAL scale and temporal precision may differ).
                if c.data_type != p.data_type
                    || c.unsigned != p.unsigned
                    || c.collation != p.collation
                {
                    return Ok(None);
                }
            }
            indexes.push(index);
        }
        for (position, (state, child_index)) in validation.fks.iter().zip(indexes).enumerate() {
            let plan = ConstraintMergePlan {
                child,
                child_index,
                parent: &state.parent,
                parent_index: &state.index,
                private: targets
                    .iter()
                    .find(|t| t.index.index_id == child_index.index_id)
                    .and_then(|t| t.file.as_ref()),
                arity: state.fk.columns.len(),
            };
            let handle = if plan.private.is_none() {
                Some(self.cached_index_handle(child.id, &child_index.name)?)
            } else {
                None
            };
            let pages = match plan.private {
                Some(file) => file.page_count(),
                None => handle.as_ref().unwrap().read().page_count(),
            };
            let requested = constraint_merge_requested_degree();
            // Small indexes do not amortize worker startup. This uses physical
            // size, not row estimates or benchmark-specific cardinalities.
            let mut degree = requested.min((pages / 32).max(1) as usize);
            // Inline scalar keys have no per-entry heap payload. Both bounded
            // batches, resume keys and all split boundaries fit well below
            // 1 MiB; buffer-pool pages/decoded caches have separate governance.
            // Keep the wide-key reservation for any non-inline suffix/INCLUDE.
            let scratch = if constraint_merge_inline_keys(plan.child, plan.child_index)
                && constraint_merge_inline_keys(plan.parent, plan.parent_index)
            {
                1024 * 1024
            } else {
                8 * 1024 * 1024
            };
            const STACK: usize = 4 * 1024 * 1024;
            let reserved = loop {
                let bytes = degree * (scratch + if degree > 1 { STACK } else { 0 });
                if validation.memory.try_grow(bytes) {
                    break bytes;
                }
                if degree == 1 {
                    return Ok(None);
                }
                degree = (degree / 2).max(1);
            };
            let result = (|| {
                let boundaries = if degree == 1 {
                    Vec::new()
                } else {
                    match plan.private {
                        Some(file) => constraint_merge_boundaries(file, plan.arity, degree)?,
                        None => constraint_merge_boundaries(
                            &handle.as_ref().unwrap().read(),
                            plan.arity,
                            degree,
                        )?,
                    }
                };
                let effective = boundaries.len() + 1;
                CONSTRAINT_INDEX_MERGE_WORKERS_PEAK.fetch_max(effective as u64, Ordering::Relaxed);
                #[cfg(test)]
                CONSTRAINT_LAST_MERGE_WORKERS.with(|n| n.set(n.get().max(effective)));
                if effective == 1 {
                    return plan.run(self, jobs, None, None);
                }
                CONSTRAINT_INDEX_MERGE_PARALLEL_RUNS.fetch_add(1, Ordering::Relaxed);
                let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
                let budget = crate::engine::executor::mem_budget::current_budget();
                std::thread::scope(|scope| {
                    let mut handles = Vec::with_capacity(effective);
                    let mut failure = None;
                    for worker in 0..effective {
                        let low = worker.checked_sub(1).map(|i| IndexBound {
                            key: boundaries[i].clone(),
                            inclusive: true,
                        });
                        let high = boundaries.get(worker).map(|key| IndexBound {
                            key: key.clone(),
                            inclusive: false,
                        });
                        let cancellation = cancellation.clone();
                        let budget = budget.clone();
                        match std::thread::Builder::new()
                            .name(format!("mydb-fk-merge-{worker}"))
                            .stack_size(STACK)
                            .spawn_scoped(scope, move || {
                                let _cancel = cancellation.enter();
                                let work = || plan.run(self, jobs, low, high);
                                match budget {
                                    Some(budget) => {
                                        crate::engine::executor::mem_budget::with_query_budget(
                                            budget, work,
                                        )
                                    }
                                    None => work(),
                                }
                            }) {
                            Ok(handle) => handles.push(handle),
                            Err(err) => {
                                failure = Some(error(
                                    crate::error::catalog::ER_UNKNOWN_ERROR,
                                    format!("cannot start FK validation worker: {err}"),
                                ));
                                break;
                            }
                        }
                    }
                    let mut combined = Some(false);
                    // Join every worker, including after an error: no worker may
                    // outlive its capture logs, descriptor pins or reservation.
                    for handle in handles {
                        match handle.join() {
                            Ok(Ok(None)) => combined = None,
                            Ok(Ok(Some(orphan))) => {
                                if let Some(value) = &mut combined {
                                    *value |= orphan;
                                }
                            }
                            Ok(Err(err)) => {
                                if failure.is_none() {
                                    failure = Some(err);
                                }
                            }
                            Err(_) => {
                                if failure.is_none() {
                                    failure = Some(error(
                                        crate::error::catalog::ER_UNKNOWN_ERROR,
                                        "FK validation worker panicked",
                                    ));
                                }
                            }
                        }
                    }
                    match failure {
                        Some(err) => Err(err),
                        None => Ok(combined),
                    }
                })
            })();
            validation.memory.shrink(reserved);
            match result? {
                None => return Ok(None),
                Some(true) => return Ok(Some(Some(position))),
                Some(false) => {}
            }
        }
        Ok(Some(None))
    }
}

#[cfg(test)]
thread_local! {
    static CONSTRAINT_MERGE_DEGREE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static CONSTRAINT_LAST_MERGE_WORKERS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn constraint_merge_requested_degree() -> usize {
    let requested = index_sort_parallel_degree();
    #[cfg(test)]
    let requested = CONSTRAINT_MERGE_DEGREE.with(|degree| degree.get().unwrap_or(requested));
    requested
        .min(std::thread::available_parallelism().map_or(1, usize::from))
        .clamp(1, 64)
}

fn constraint_merge_inline_keys(table: &TableSchema, index: &IndexSchema) -> bool {
    if index.columns.len() + index.included_columns.len() > 4 {
        return false;
    }
    index
        .columns
        .iter()
        .chain(&index.included_columns)
        .all(|name| {
            let Ok(position) = table.column_index(name) else {
                return false;
            };
            match &table.columns[position].data_type {
                DataTypeKind::Bool
                | DataTypeKind::TinyInt
                | DataTypeKind::SmallInt
                | DataTypeKind::MediumInt
                | DataTypeKind::Int
                | DataTypeKind::BigInt
                | DataTypeKind::Float { .. }
                | DataTypeKind::Double { .. }
                | DataTypeKind::Date
                | DataTypeKind::Time { .. }
                | DataTypeKind::DateTime { .. }
                | DataTypeKind::Timestamp { .. }
                | DataTypeKind::Year => true,
                DataTypeKind::Decimal {
                    precision: Some(precision),
                    ..
                } => *precision <= 38,
                _ => false,
            }
        })
}

// Approximate quantiles by descending the B-tree's child fanout. This reads
// O(degree * height) pages, never a serial pass over every leaf. Unequal subtree
// sizes affect balance only. Boundaries are FK prefixes, so duplicate groups
// never straddle ranges, including nonunique supporting-index suffixes.
fn constraint_merge_boundaries(
    file: &IndexFile,
    arity: usize,
    degree: usize,
) -> Result<Vec<IndexKey>, QueryError> {
    let _structure = file.structure_lock.read_recursive();
    let mut boundaries = Vec::with_capacity(degree - 1);
    for ordinal in 1..degree {
        let mut fraction = ordinal;
        let mut page_id = file.root_page.load(Ordering::Acquire);
        for depth in 0..64 {
            crate::engine::cancel::check_cancelled()?;
            match file.read_page(page_id)?.as_ref() {
                IndexPage::Internal(page) if !page.children.is_empty() => {
                    let offset = fraction * page.children.len();
                    page_id = page.children[offset / degree];
                    fraction = offset % degree;
                }
                IndexPage::Leaf(page) => {
                    if let Some(entry) = page.entries.get(fraction * page.entries.len() / degree) {
                        let parts = entry.key.0.get(..arity).ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "truncated FK split key",
                            )
                        })?;
                        boundaries.push(IndexKey::from(parts.to_vec()));
                    }
                    break;
                }
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "invalid FK index tree",
                    ))
                }
            }
            if depth == 63 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "FK index tree is too deep",
                ));
            }
        }
    }
    boundaries.sort_unstable();
    boundaries.dedup();
    Ok(boundaries)
}

#[derive(Clone, Copy)]
struct ConstraintMergePlan<'a> {
    child: &'a TableSchema,
    child_index: &'a IndexSchema,
    parent: &'a TableSchema,
    parent_index: &'a IndexSchema,
    private: Option<&'a IndexFile>,
    arity: usize,
}

impl ConstraintMergePlan<'_> {
    // None means the speculative read was invalidated. A violation is only
    // actionable after the coordinator certifies the unchanged capture window.
    fn run(
        &self,
        db: &Database,
        jobs: &[Arc<AlterCopyJob>],
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    ) -> Result<Option<bool>, QueryError> {
        let mut children = ConstraintIndexCursor {
            public: if self.private.is_none() {
                Some(db.range_index_entries_cursor(
                    self.child,
                    self.child_index,
                    low.clone(),
                    high.clone(),
                )?)
            } else {
                None
            },
            building: self.private,
            resume: None,
            low: low.clone(),
            high: high.clone(),
        };
        let mut parents =
            db.range_index_entries_cursor(self.parent, self.parent_index, low, high)?;
        let mut parent_batch = Vec::new();
        let mut parent_pos = 0;
        loop {
            crate::engine::cancel::check_cancelled()?;
            if !constraint_capture_is_empty(jobs)? {
                return Ok(None);
            }
            let batch = children.next()?;
            if batch.is_empty() {
                return Ok(Some(false));
            }
            CONSTRAINT_INDEX_MERGE_ENTRIES.fetch_add(batch.len() as u64, Ordering::Relaxed);
            for (key, _) in &batch {
                let parts = key.0.get(..self.arity).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "truncated FK child index key",
                    )
                })?;
                if parts.iter().any(|p| matches!(p, KeyPart::Null)) {
                    continue;
                }
                loop {
                    if parent_pos == parent_batch.len() {
                        crate::engine::cancel::check_cancelled()?;
                        if !constraint_capture_is_empty(jobs)? {
                            return Ok(None);
                        }
                        parent_batch.clear();
                        parent_batch = parents.next_chunk(128)?;
                        parent_pos = 0;
                        CONSTRAINT_INDEX_MERGE_ENTRIES
                            .fetch_add(parent_batch.len() as u64, Ordering::Relaxed);
                        if parent_batch.is_empty() {
                            return Ok(Some(true));
                        }
                    }
                    let parent_parts =
                        parent_batch[parent_pos]
                            .0
                             .0
                            .get(..self.arity)
                            .ok_or_else(|| {
                                error(
                                    crate::error::catalog::ER_GET_ERRNO,
                                    "truncated FK parent index key",
                                )
                            })?;
                    match parent_parts.cmp(parts) {
                        std::cmp::Ordering::Less => parent_pos += 1,
                        std::cmp::Ordering::Equal => break,
                        std::cmp::Ordering::Greater => return Ok(Some(true)),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
impl Database {
    pub(crate) fn with_constraint_merge_degree_for_test<T>(
        &self,
        degree: usize,
        f: impl FnOnce() -> T,
    ) -> T {
        struct Reset(Option<usize>);
        impl Drop for Reset {
            fn drop(&mut self) {
                CONSTRAINT_MERGE_DEGREE.with(|n| n.set(self.0));
            }
        }
        let _reset = Reset(CONSTRAINT_MERGE_DEGREE.with(|n| n.replace(Some(degree))));
        f()
    }
    pub(crate) fn last_constraint_merge_workers_for_test(&self) -> usize {
        CONSTRAINT_LAST_MERGE_WORKERS.with(|n| n.get())
    }
}

#[cfg(test)]
mod constraint_merge_memory_tests {
    use super::*;

    #[test]
    fn fk_merge_inline_reservation_checks_full_key_and_included_columns() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::engine::StorageQueryEngine::open(dir.path()).unwrap();
        engine.execute("CREATE TABLE t(a INT,b DECIMAL(30,2),c DECIMAL(50,2),d VARCHAR(30),INDEX scalar_key(a,b),INDEX wide_key(a,c),INDEX suffix_key(a,d))").unwrap();
        let table = engine.database().table("test.t").unwrap();
        let find = |name: &str| table.indexes.iter().find(|i| i.name == name).unwrap();
        assert!(constraint_merge_inline_keys(&table, find("scalar_key")));
        assert!(!constraint_merge_inline_keys(&table, find("wide_key")));
        assert!(!constraint_merge_inline_keys(&table, find("suffix_key")));
        let mut included = find("scalar_key").clone();
        included.included_columns.push("d".to_string());
        assert!(!constraint_merge_inline_keys(&table, &included));
        let mut many = find("scalar_key").clone();
        many.columns = vec!["a".to_string(); 5];
        assert!(!constraint_merge_inline_keys(&table, &many));
    }
}
