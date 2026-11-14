//! Optional sparse membership discovery in a narrow non-leading B-tree key.
//! Every candidate is fetched and rechecked by the existing heap/MVCC reader.
use super::*;
use crate::engine::executor::select::plan as scan_plan;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};

#[cfg(test)]
thread_local! {
    static BEFORE_PUBLISH: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

pub(super) struct NarrowChoice {
    index: IndexSchema,
    key_position: usize,
    filter: Arc<crate::engine::executor::select::bloom::ExactIntKeySet>,
    max_ids: usize,
    workspace: usize,
    estimated_pages: u64,
}
impl RuntimeIndexScan<'_> {
    pub(super) fn narrow_choice(&self) -> Option<NarrowChoice> {
        if !self.allowed
            || self.opened
            || self.txn.historical
            || self.txn.dirty_read
            || self.table.system_time.is_some()
            || self.table.schema.is_partitioned()
            || self.db.has_pending_index_repairs()
        {
            return None;
        }
        let budget = mem_budget::current_budget()?;
        let workspace = (budget.limit() / 3).min(256 << 20);
        let rows = self
            .table
            .schema
            .stats
            .row_count
            .saturating_add(self.db.table_modification_delta(self.table.schema.id));
        let pages = self.table.schema.stats.heap_page_count;
        // Keep cache-resident heaps on their native predicate-first scan.
        if pages.saturating_mul(crate::storage::PAGE_SIZE as u64)
            <= mem_budget::effective_memory_limit_bytes() as u64
        {
            return None;
        }
        for filter in &self.filters {
            scan_plan::compile_runtime_filter_probe_projected(
                filter,
                &self.table.schema,
                &self.columns,
            )?;
        }
        let mut best: Option<NarrowChoice> = None;
        for candidate in &self.filters {
            let RuntimeFilter::ExactIntKey {
                keys,
                null_safe,
                filter,
                ..
            } = candidate
            else {
                continue;
            };
            if keys.len() != 1
                || null_safe.iter().any(|v| *v)
                || filter.len() == 0
                || filter.len() > workspace / 256
            {
                continue;
            }
            let columns = scan_plan::runtime_filter_schema_indices(candidate, &self.table)?;
            let [column] = columns.as_slice() else {
                continue;
            };
            let column = &self.table.schema.columns[*column];
            use crate::storage::DataTypeKind as T;
            if column.unsigned
                || !matches!(
                    column.data_type,
                    T::TinyInt | T::SmallInt | T::MediumInt | T::Int | T::BigInt
                )
            {
                continue;
            }
            let ndv = self
                .table
                .schema
                .stats
                .columns
                .get(&column.name.to_ascii_lowercase())
                .map_or(0, |s| s.distinct_count);
            let Some(entries) =
                scan_plan::estimate_runtime_keyset_index_entries(rows, ndv, filter.len())
            else {
                continue;
            };
            if entries > (rows / 128) as usize {
                continue;
            }
            // Cardinality estimates price the attempt, not its correctness.
            // Leave bounded room for skew and retained index versions; the
            // actual distinct-page proof still decides whether to publish.
            let max_ids = workspace / 64;
            if max_ids < entries {
                continue;
            }
            for index in self.table.schema.iter_queryable_indexes() {
                if index.is_fulltext()
                    || index.is_spatial()
                    || index.is_vector()
                    || index.has_expression_parts()
                    || index.has_prefix_columns()
                    || index.has_multi_valued_parts()
                    || index.readable_from_scn > self.txn.snapshot_scn
                {
                    continue;
                }
                let Some(key_position) = index
                    .columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(&column.name))
                else {
                    continue;
                };
                if key_position == 0 {
                    continue;
                }
                let stats = self
                    .table
                    .schema
                    .stats
                    .indexes
                    .get(&index.name.to_ascii_lowercase());
                let Some(estimated_pages) = stats.and_then(|s| s.leaf_page_count).or_else(|| {
                    index
                        .columns
                        .iter()
                        .all(|name| {
                            self.table.schema.column_index(name).ok().is_some_and(|c| {
                                matches!(
                                    self.table.schema.columns[c].data_type,
                                    T::TinyInt | T::SmallInt | T::MediumInt | T::Int | T::BigInt
                                )
                            })
                        })
                        .then(|| {
                            rows.saturating_mul((index.columns.len() * 18 + 24) as u64)
                                .div_ceil(crate::storage::PAGE_SIZE as u64)
                        })
                }) else {
                    continue;
                };
                if estimated_pages.saturating_mul(2) >= pages
                    || estimated_pages.saturating_add(entries as u64) > pages.saturating_mul(3) / 4
                {
                    continue;
                }
                if best
                    .as_ref()
                    .is_some_and(|b| b.estimated_pages <= estimated_pages)
                {
                    continue;
                }
                best = Some(NarrowChoice {
                    index: index.clone(),
                    key_position,
                    filter: filter.clone(),
                    max_ids,
                    workspace,
                    estimated_pages,
                });
            }
        }
        best
    }

    pub(super) fn start_narrow(&mut self, choice: NarrowChoice) -> Result<(), QueryError> {
        self.db
            .check_index_read_view(&self.table.schema, &choice.index, self.txn)?;
        let heap_pages = self.db.heap_page_count_for_storage(self.table.schema.id)?;
        let index_pages = self
            .db
            .index_page_count(self.table.schema.id, &choice.index.name);
        if index_pages == 0 || index_pages.saturating_mul(2) >= heap_pages {
            return Ok(());
        }
        let degree = self.scan_degree().min(16);
        let bytes = usize::try_from(index_pages)
            .ok()
            .and_then(|n| n.checked_mul(16))
            .and_then(|n| {
                n.checked_add(
                    choice
                        .max_ids
                        .saturating_mul(3 * std::mem::size_of::<RowId>() + 8),
                )
            })
            .and_then(|n| n.checked_add(degree * 65536 + 4096));
        let Some(bytes) = bytes.filter(|n| *n <= choice.workspace) else {
            return Ok(());
        };
        let Some(workspace) = mem_budget::ControlledMemoryCharge::try_new_optional(
            mem_budget::ControlledMemoryCategory::Executor,
            bytes,
        ) else {
            return Ok(());
        };
        let version = self.db.index_repair_version();
        let rollback_epoch = self
            .db
            .index_repair_rollback_epoch(self.table.schema.id, &choice.index.name);
        if version & 1 != 0 || self.db.has_pending_index_repairs() {
            return Ok(());
        }
        let leaves = self
            .db
            .range_index_leaf_ids(&self.table.schema, &choice.index, None, None)?;
        if leaves.is_empty() {
            return Ok(());
        }
        let degree = degree.min(leaves.len());
        let chunk = leaves.len().div_ceil(degree);
        let context =
            crate::engine::executor::select::execution_context::ExecutionContext::capture(self.txn);
        let _pressure = (degree > 1)
            .then(|| context.enter_parallel_read_pressure())
            .transpose()?;
        let count = AtomicUsize::new(0);
        let examined = AtomicUsize::new(0);
        let refused = AtomicBool::new(false);
        let max_examined = self
            .table
            .schema
            .stats
            .row_count
            .saturating_add(self.db.table_modification_delta(self.table.schema.id))
            .saturating_mul(2)
            .max(1);
        let results = crate::util::thread_spawn::scope(|scope| {
            let mut handles = Vec::with_capacity(degree);
            for start in (0..leaves.len()).step_by(chunk) {
                let end = (start + chunk).min(leaves.len());
                let seed = (start > 0).then(|| leaves[start - 1]);
                let db = &self.db;
                let table = &self.table.schema;
                let choice = &choice;
                let leaves = &leaves;
                let (count, examined, refused) = (&count, &examined, &refused);
                let context = context.clone();
                handles.push(scope.spawn(move || {
                    context.run(db, || {
                        let mut ids = Vec::new();
                        let mut aborted = false;
                        let result = db.visit_index_lossless_i64_leaf_entry_chunks_with_bound(
                            table,
                            &choice.index,
                            &leaves[start..end],
                            seed,
                            None,
                            None,
                            choice.key_position,
                            |entries, _| {
                                crate::engine::cancel::check_cancelled()?;
                                let scanned = examined
                                    .fetch_add(entries.len(), Relaxed)
                                    .saturating_add(entries.len());
                                if refused.load(Relaxed)
                                    || scanned as u64 > max_examined
                                    || db.index_repair_version() != version
                                {
                                    refused.store(true, Relaxed);
                                    aborted = true;
                                    return Err(crate::error::catalog::ER_INTERNAL_ERROR
                                        .error("optional narrow-index discovery declined"));
                                }
                                for (key, id) in entries {
                                    if !key.is_some_and(|k| choice.filter.contains(i128::from(k))) {
                                        continue;
                                    }
                                    if count.fetch_add(1, Relaxed) >= choice.max_ids
                                        || ids.try_reserve(1).is_err()
                                    {
                                        refused.store(true, Relaxed);
                                        aborted = true;
                                        return Err(crate::error::catalog::ER_INTERNAL_ERROR
                                            .error("optional narrow-index discovery declined"));
                                    }
                                    ids.push(*id);
                                }
                                Ok(())
                            },
                        );
                        // Only the locally generated refusal is swallowed. I/O and
                        // cancellation errors from another worker remain errors.
                        if aborted {
                            Ok(None)
                        } else {
                            result.map(|_| Some(ids))
                        }
                    })
                }));
            }
            handles
                .into_iter()
                .map(|h| {
                    h.join().unwrap_or_else(|_| {
                        Err(crate::error::catalog::ER_INTERNAL_ERROR
                            .error("narrow-index worker panicked"))
                    })
                })
                .collect::<Vec<_>>()
        });
        let mut batches = Vec::with_capacity(results.len());
        let mut declined = false;
        for result in results {
            match result? {
                Some(ids) => batches.push(ids),
                None => declined = true,
            }
        }
        crate::engine::cancel::check_cancelled()?;
        drop(leaves);
        #[cfg(test)]
        BEFORE_PUBLISH.with(|hook| {
            if let Some(action) = hook.borrow_mut().take() {
                action();
            }
        });
        analyze_stats::add_range_scan_work(examined.load(Relaxed) as u64, 0, 0);
        if declined
            || self.db.index_repair_version() != version
            || self.db.has_pending_index_repairs()
            || self
                .db
                .index_repair_rollback_epoch(self.table.schema.id, &choice.index.name)
                != rollback_epoch
        {
            analyze_stats::add_runtime_index_candidates(1, 0, true);
            return Ok(());
        }
        let mut ids = Vec::new();
        let total = batches.iter().map(Vec::len).sum();
        if ids.try_reserve_exact(total).is_err() {
            return Ok(());
        }
        for batch in batches {
            ids.extend(batch)
        }
        ids.sort_unstable();
        ids.dedup();
        let pages = ids
            .iter()
            .map(|id| id.page_id)
            .fold((None, 0u64), |(previous, n), page| {
                (Some(page), n + u64::from(previous != Some(page)))
            })
            .1;
        // Discovery estimates RIDs; publication proves actual physical work.
        if index_pages.saturating_add(pages) > heap_pages.saturating_mul(3) / 4 {
            analyze_stats::add_runtime_index_candidates(1, ids.len() as u64, true);
            return Ok(());
        }
        analyze_stats::add_runtime_index_candidates(1, ids.len() as u64, false);
        let indexed = vec_index_scan::IndexBatchStream::from_row_ids(
            &self.db,
            self.txn,
            &self.table,
            &choice.index,
            ids,
            Some(&self.columns),
        )?;
        let Some(indexed) = indexed else {
            return Ok(());
        };
        self.probes = self
            .filters
            .iter()
            .map(|f| {
                scan_plan::compile_runtime_filter_probe_projected(
                    f,
                    &self.table.schema,
                    &self.columns,
                )
            })
            .collect();
        drop(workspace);
        self.adopt_indexed(indexed, pages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{executor::select::bloom::ExactIntKeySetBuilder, StorageQueryEngine};
    struct Fallback {
        inner: MaterializedBatchOperator,
        pulls: Arc<AtomicUsize>,
    }
    impl BatchOperator for Fallback {
        fn schema(&self) -> &Arc<BatchSchema> {
            self.inner.schema()
        }
        fn install_runtime_filter(&mut self, _: RuntimeFilter) -> bool {
            true
        }
        fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
            self.pulls.fetch_add(1, Relaxed);
            self.inner.next_batch()
        }
    }
    #[test]
    fn sparse_nonleading_integer_domain_fetches_candidates_and_keeps_fallback_unopened() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let padding = (0..64)
            .map(|n| format!("p{n} BIGINT"))
            .collect::<Vec<_>>()
            .join(",");
        engine.execute(&format!("CREATE TABLE narrow_rows(bucket INT,k BIGINT,v VARCHAR(3000),{padding},PRIMARY KEY(bucket,k))")).unwrap();
        let values = (0..200)
            .map(|n| {
                format!(
                    "({}, {}, '{}',{})",
                    n % 10,
                    n,
                    "x".repeat(2000),
                    std::iter::repeat_n("123456789", 64)
                        .collect::<Vec<_>>()
                        .join(",")
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO narrow_rows VALUES {values}"))
            .unwrap();
        engine.execute("ANALYZE TABLE narrow_rows").unwrap();
        let db = engine.db_for_tests();
        db.set_undo_retention_ms(0).unwrap();
        db.gc_committed_txn_slots();
        let snapshot = db.visible_scn();
        let _snapshot_guard = db.register_statement_snapshot(snapshot);
        for mode in [
            "eligible",
            "dense",
            "cache",
            "dirty",
            "historical",
            "nullsafe",
            "capacity",
            "workspace",
            "pages",
            "residual",
            "cancel",
            "snapshot",
            "rollback",
        ] {
            if mode == "snapshot" {
                engine
                    .execute("UPDATE narrow_rows SET v='newer' WHERE bucket=7 AND k=37")
                    .unwrap();
            }
            let budget = Arc::new(mem_budget::MemBudget::new(64 << 20));
            mem_budget::with_query_budget(budget.clone(), || {
                let mut table =
                    PlanTable::new(1, db.table("narrow_rows").unwrap(), Some("r".into()));
                let stats = &mut Arc::make_mut(&mut table.schema).stats;
                stats.row_count = 1_000_000;
                stats.heap_page_count = 1 << 24;
                stats.columns.get_mut("k").unwrap().distinct_count = 1_000_000;
                let schema = super::super::build_scan_batch_schema(&table, &[1, 2]);
                let selected = if mode == "pages" {
                    (0..200).collect::<Vec<_>>()
                } else if mode == "residual" {
                    vec![144]
                } else {
                    vec![37, 144]
                };
                let expected = selected
                    .into_iter()
                    .map(|k| vec![Value::Int(k), Value::String("x".repeat(2000))])
                    .collect::<Vec<_>>();
                let batch = Batch::new(
                    schema.clone(),
                    vec![
                        ColumnVector::from_values(expected.iter().map(|r| r[0].clone()).collect()),
                        ColumnVector::from_values(expected.iter().map(|r| r[1].clone()).collect()),
                    ],
                );
                let pulls = Arc::new(AtomicUsize::new(0));
                let original = Box::new(Fallback {
                    inner: MaterializedBatchOperator::new(schema.clone(), vec![batch]),
                    pulls: pulls.clone(),
                });
                let mut keys =
                    ExactIntKeySetBuilder::with_capacity(if mode == "pages" { 200 } else { 2 })
                        .unwrap();
                if mode == "pages" {
                    for k in 0..200 {
                        keys.insert(k);
                    }
                } else {
                    keys.insert(37);
                    keys.insert(144);
                }
                let filter = RuntimeFilter::ExactIntKey {
                    keys: vec![Expr::CompoundIdentifier(vec!["r".into(), "k".into()])],
                    null_safe: vec![false],
                    filter: Arc::new(keys.finish().unwrap()),
                    rejected: Arc::new(std::sync::OnceLock::new()),
                };
                if mode == "eligible" {
                    let crate::parser::Statement::Select(q) = crate::parser::parse_statement(
                        "SELECT k,v FROM narrow_rows r WHERE r.k IS NOT NULL",
                    )
                    .unwrap() else {
                        panic!()
                    };
                    for hint in [None, Some("FULL_SCAN")] {
                        let plan = crate::planner::PhysicalPlan::ParallelSeqScan {
                            table: table.clone(),
                            filter: q.selection.clone(),
                            projected_columns: Some(vec![1, 2].into()),
                            runtime_filters: vec![],
                            estimated_rows: 1_000_000.0,
                            access_rows: 1_000_000.0,
                            possible_keys: vec![],
                            cost: crate::engine::cbo::Cost::scalar(1000.0),
                            hint_note: hint.map(str::to_owned),
                            degree: 2,
                            output_columns_in_projection: None,
                        };
                        let txn = TxnView::read_only(
                            snapshot,
                            crate::storage::IsolationLevel::ReadCommitted,
                        );
                        let mut input = super::super::try_build_direct_batch_scan(&db, txn, &plan)
                            .unwrap()
                            .unwrap();
                        assert!(input.stream.install_runtime_filter(filter.clone()));
                        assert_eq!(input.stream.properties().partitioning == BatchProperties::UNORDERED_SINGLE.partitioning, hint.is_none(),
                            "a late exact domain must reach a nonselective local filter without bypassing explicit access: {hint:?}");
                        let mut actual = Vec::new();
                        while let Some(batch) = input.stream.next_batch().unwrap() {
                            actual.extend(batch.visible_indices().map(|row| batch.row(row)));
                        }
                        actual.sort_by_key(|r| match r[0] {
                            Value::Int(k) => k,
                            _ => panic!(),
                        });
                        assert_eq!(actual, expected);
                    }
                }
                let mut source = RuntimeIndexScan {
                    db: db.clone(),
                    txn: TxnView::read_only(
                        snapshot,
                        crate::storage::IsolationLevel::ReadCommitted,
                    ),
                    table,
                    columns: vec![1, 2],
                    schema,
                    original,
                    filters: vec![filter],
                    indexed: None,
                    probes: Vec::new(),
                    opened: false,
                    allowed: true,
                    residual: None,
                };
                if mode == "rollback" {
                    let engine = engine.clone();
                    BEFORE_PUBLISH.with(|hook| {
                        *hook.borrow_mut() = Some(Box::new(move || {
                            engine.execute("START TRANSACTION").unwrap();
                            engine
                                .execute("UPDATE narrow_rows SET k=1000037 WHERE bucket=7 AND k=37")
                                .unwrap();
                            engine.execute("ROLLBACK").unwrap();
                        }))
                    });
                }
                assert!(source.choice().is_none());
                assert!(source.intersection_choice().is_none());
                match mode {
                    "dense" => {
                        Arc::make_mut(&mut source.table.schema)
                            .stats
                            .columns
                            .get_mut("k")
                            .unwrap()
                            .distinct_count = 1
                    }
                    "cache" => {
                        Arc::make_mut(&mut source.table.schema)
                            .stats
                            .heap_page_count = 10
                    }
                    "dirty" => source.txn.dirty_read = true,
                    "historical" => source.txn.historical = true,
                    "nullsafe" => {
                        if let RuntimeFilter::ExactIntKey { null_safe, .. } = &mut source.filters[0]
                        {
                            *null_safe = vec![true]
                        }
                    }
                    "residual" => {
                        let crate::parser::Statement::Select(q) = crate::parser::parse_statement(
                            "SELECT k FROM narrow_rows r WHERE r.k>100",
                        )
                        .unwrap() else {
                            panic!()
                        };
                        source.residual =
                            crate::engine::executor::vec::expr_batch::BatchExprProgram::compile(
                                q.selection.as_ref().unwrap(),
                                &source.schema,
                            )
                            .unwrap();
                    }
                    _ => {}
                }
                let declined = matches!(
                    mode,
                    "dense" | "cache" | "dirty" | "historical" | "nullsafe"
                );
                assert_eq!(source.narrow_choice().is_none(), declined, "{mode}");
                if matches!(mode, "capacity" | "workspace" | "cancel") {
                    let mut choice = source.narrow_choice().unwrap();
                    if mode == "capacity" {
                        choice.max_ids = 1;
                    }
                    if mode == "workspace" {
                        choice.workspace = 0;
                    }
                    source.opened = true;
                    if mode == "cancel" {
                        let token = crate::engine::cancel::QueryCancelToken::noop().fork_local();
                        let _scope = crate::engine::cancel::enter(token.clone());
                        token.cancel_local();
                        assert_eq!(source.start_narrow(choice).unwrap_err().code(), 1317);
                        assert_eq!(pulls.load(Relaxed), 0);
                        return;
                    }
                    source.start_narrow(choice).unwrap();
                    assert!(source.indexed.is_none(), "{mode}");
                }
                let mut rows = Vec::new();
                while let Some(batch) = source.next_batch().unwrap() {
                    for n in batch.visible_indices() {
                        rows.push(batch.row(n))
                    }
                }
                rows.sort_by_key(|r| match r[0] {
                    Value::Int(k) => k,
                    _ => panic!(),
                });
                assert_eq!(rows, expected);
                assert_eq!(
                    pulls.load(Relaxed) > 0,
                    declined || matches!(mode, "capacity" | "workspace" | "pages" | "rollback"),
                    "{mode}"
                );
            });
            assert_eq!(budget.used(), 0, "{mode}");
        }
    }
}
