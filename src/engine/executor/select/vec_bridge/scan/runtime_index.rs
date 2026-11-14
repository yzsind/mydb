//! Optional bounded index candidates for an unopened heap scan.
use super::*;
use crate::planner::{PlanTable, RuntimeFilter};
use crate::storage::{IndexSchema, RowId};
mod intersection;
mod local;
mod narrow;

struct Choice {
    index: IndexSchema,
    keys: Arc<crate::engine::executor::select::bloom::ExactIntKeySet>,
    column: usize,
    max_entries: usize,
    bytes: usize,
}

/// Admit a sparse row domain into a bounded RID workspace, then require a
/// physical-page advantage after sorting. Rows sharing a heap page must not
/// be priced as independent page probes before their locality is known.
fn entry_limit(
    rows: u64,
    pages: u64,
    ndv: u64,
    keys: usize,
    workspace: usize,
    allow_locality_discovery: bool,
    constant_domain: bool,
) -> Option<usize> {
    let entries =
        super::super::super::plan::estimate_runtime_keyset_index_entries(rows, ndv, keys)?;
    // A single exact membership test is cheap in native scan workers. Keep
    // its stronger page advantage: serial RID fetch can lose even at sparse
    // row selectivity. Complete tuple filters also hash multiple lanes for
    // every scanned row. A heap exceeding host/cgroup memory also necessarily
    // pays cold-page traffic. Admit bounded locality discovery for that work.
    let page_limit = if constant_domain && allow_locality_discovery {
        // An exact local predicate may cluster many qualifying rows on each
        // selected page. Discover its real footprint inside the same RID
        // workspace and row-fraction bounds; adoption still requires fewer
        // than half the original heap pages after MVCC-safe RID collection.
        rows / 16
    } else if allow_locality_discovery {
        pages.saturating_mul(2)
    } else {
        pages / 2
    };
    if pages == 0
        || entries > usize::try_from(page_limit).unwrap_or(usize::MAX)
        || entries > usize::try_from((rows / 16).max(1)).unwrap_or(usize::MAX)
    {
        return None;
    }
    let per_entry = std::mem::size_of::<(u32, RowId)>() + 2 * std::mem::size_of::<RowId>() + 8;
    let cap = workspace.checked_sub(keys.checked_mul(256)?)? / per_entry;
    let limit = entries.checked_mul(2)?.max(1).min(cap);
    (limit >= entries).then_some(limit)
}

pub(super) struct RuntimeIndexScan<'a> {
    db: Arc<Database>,
    txn: TxnView,
    table: PlanTable,
    columns: Vec<usize>,
    schema: Arc<BatchSchema>,
    original: Box<dyn BatchOperator + 'a>,
    filters: Vec<RuntimeFilter>,
    indexed: Option<Box<dyn BatchOperator + 'a>>,
    probes: Vec<Option<super::super::super::plan::CompiledRuntimeFilterProbe>>,
    opened: bool,
    allowed: bool,
    residual: Option<Arc<expr_batch::BatchExprProgram>>,
}

impl RuntimeIndexScan<'_> {
    fn scan_degree(&self) -> usize {
        // A full batch predicate can hide native-worker extraction while its
        // properties still retain the physical scan's concurrency ceiling.
        match self.original.properties().partitioning {
            BatchPartitioning::RoundRobin { partitions } => partitions.max(1),
            _ => 1,
        }
    }

    fn adopt_indexed(
        &mut self,
        indexed: vec_index_scan::IndexBatchStream,
        pages: u64,
    ) -> Result<(), QueryError> {
        // Retain the planner's concurrency ceiling. Small domains keep the
        // serial lane; large page-disjoint domains reuse bounded scan queues,
        // captured snapshots, cancellation and the existing MRR governor.
        let degree = if pages >= 4096 { self.scan_degree() } else { 1 };
        let mut workers = indexed.into_row_id_workers(degree)?;
        self.indexed = Some(if workers.len() > 1 {
            Box::new(super::ParallelIndexBatchStream::new(
                &self.db, self.txn, workers,
            ))
        } else {
            Box::new(
                workers
                    .pop()
                    .expect("RID source retains at least one reader"),
            )
        });
        Ok(())
    }

    fn other_exact_survival(&self, selected: &RuntimeFilter, column: usize) -> f64 {
        let mut survival = 1.0;
        for other in &self.filters {
            if std::ptr::eq(other, selected) {
                continue;
            }
            let RuntimeFilter::ExactIntKey { filter, .. } = other else {
                continue;
            };
            let Some(indices) =
                super::super::super::plan::runtime_filter_schema_indices(other, &self.table)
            else {
                return 0.0;
            };
            let [other_column] = indices.as_slice() else {
                return 0.0;
            };
            if *other_column == column {
                if matches!(selected, RuntimeFilter::ExactIntKey { filter: selected, .. } if Arc::ptr_eq(filter, selected))
                {
                    continue;
                }
                return 0.0;
            }
            let name = &self.table.schema.columns[*other_column].name;
            let ndv = self
                .table
                .schema
                .stats
                .columns
                .get(&name.to_ascii_lowercase())
                .map_or(0, |stats| stats.distinct_count);
            if ndv == 0 {
                return 0.0;
            }
            survival *= (filter.len() as f64 / ndv as f64).clamp(0.0, 1.0);
        }
        survival
    }

    fn choice(&self) -> Option<Choice> {
        if !self.allowed
            || self.opened
            || self.table.schema.is_partitioned()
            || self.table.system_time.is_some()
            || self.txn.historical
            || self.db.has_pending_index_repairs()
        {
            return None;
        }
        let budget = mem_budget::current_budget()?;
        let workspace = (budget.limit() / 3).min(256 << 20);
        let composite_filter = self.filters.iter().any(|filter| {
            matches!(filter, RuntimeFilter::BloomKey { keys, .. } | RuntimeFilter::KeySet { keys, .. } if keys.len() > 1)
        });
        let heap_bytes = self
            .table
            .schema
            .stats
            .heap_page_count
            .saturating_mul(crate::storage::PAGE_SIZE as u64);
        let allow_locality_discovery = composite_filter
            || heap_bytes > mem_budget::effective_memory_limit_bytes() as u64
            || (self.residual.is_some() && heap_bytes > local::residency_limit());
        // All filters must be checkable against the candidate projection. A
        // scan's private predicate-only columns need the original reader.
        for filter in &self.filters {
            super::super::super::plan::compile_runtime_filter_probe_projected(
                filter,
                &self.table.schema,
                &self.columns,
            )?;
        }
        let mut best: Option<Choice> = None;
        for runtime_filter in &self.filters {
            let RuntimeFilter::ExactIntKey {
                keys,
                null_safe,
                filter,
                ..
            } = runtime_filter
            else {
                continue;
            };
            if keys.len() != 1
                || null_safe.first().copied().unwrap_or(false)
                || filter.len() == 0
                || filter.len() > workspace / 256
            {
                continue;
            }
            let indices = super::super::super::plan::runtime_filter_schema_indices(
                runtime_filter,
                &self.table,
            )?;
            let column = indices[0];
            // Native scan workers test every exact domain before decoding
            // payload columns. RID fetch decodes first. Keep the strict page
            // bound when other domains would discard most index candidates.
            let allow_locality_discovery = allow_locality_discovery
                && self.other_exact_survival(runtime_filter, column) >= 0.5;
            if !matches!(
                self.table.schema.columns[column].data_type,
                crate::storage::DataTypeKind::TinyInt
                    | crate::storage::DataTypeKind::SmallInt
                    | crate::storage::DataTypeKind::MediumInt
                    | crate::storage::DataTypeKind::Int
                    | crate::storage::DataTypeKind::BigInt
            ) {
                continue;
            }
            for index in self.table.schema.iter_queryable_indexes() {
                if index.is_fulltext()
                    || index.is_spatial()
                    || index.is_vector()
                    || index.has_multi_valued_parts()
                    || index.has_expression_parts()
                    || index.has_prefix_columns()
                    || !index.columns.first().is_some_and(|c| {
                        c.eq_ignore_ascii_case(&self.table.schema.columns[column].name)
                    })
                {
                    continue;
                }
                let ndv = self
                    .table
                    .schema
                    .index_prefix_cardinality(&index.name, 1)
                    .unwrap_or(0);
                let rows = self
                    .table
                    .schema
                    .stats
                    .row_count
                    .saturating_add(self.db.table_modification_delta(self.table.schema.id));
                let Some(max_entries) = entry_limit(
                    rows,
                    self.table.schema.stats.heap_page_count,
                    ndv,
                    filter.len(),
                    workspace,
                    allow_locality_discovery,
                    self.residual.is_some(),
                ) else {
                    continue;
                };
                if best
                    .as_ref()
                    .is_some_and(|best| best.max_entries <= max_entries)
                {
                    continue;
                }
                best = Some(Choice {
                    index: index.clone(),
                    keys: filter.clone(),
                    column,
                    max_entries,
                    bytes: max_entries
                        .saturating_mul(
                            std::mem::size_of::<(u32, RowId)>()
                                + 2 * std::mem::size_of::<RowId>()
                                + 8,
                        )
                        .saturating_add(filter.len().saturating_mul(256)),
                });
            }
        }
        best
    }

    fn start(&mut self) -> Result<(), QueryError> {
        let choice = self.choice();
        let intersection = choice
            .is_none()
            .then(|| self.intersection_choice())
            .flatten();
        let narrow = (choice.is_none() && intersection.is_none())
            .then(|| self.narrow_choice())
            .flatten();
        self.opened = true;
        if let Some(intersection) = intersection {
            return self.start_intersection(intersection);
        }
        if let Some(narrow) = narrow {
            return self.start_narrow(narrow);
        }
        let Some(choice) = choice else {
            return Ok(());
        };
        // Optional reservations never latch a terminal allocation failure.
        let Some(workspace) = mem_budget::ControlledMemoryCharge::try_new_optional(
            mem_budget::ControlledMemoryCategory::Executor,
            choice.bytes,
        ) else {
            return Ok(());
        };
        let column = &self.table.schema.columns[choice.column];
        let mut keys = Vec::with_capacity(choice.keys.len());
        let mut valid = true;
        choice.keys.for_each_key(|key| {
            let value = if column.unsigned {
                match u64::try_from(key) {
                    Ok(key) => Value::UInt(key),
                    Err(_) => {
                        valid = false;
                        return;
                    }
                }
            } else {
                match i64::try_from(key) {
                    Ok(key) => Value::Int(key),
                    Err(_) => {
                        valid = false;
                        return;
                    }
                }
            };
            let part = match value {
                Value::Int(key) => crate::storage::KeyPart::Int(key),
                Value::UInt(key) => crate::storage::KeyPart::UInt(key),
                _ => unreachable!("native integer index key"),
            };
            keys.push(crate::storage::IndexKey::from(vec![part]));
        });
        if !valid {
            return Ok(());
        }
        keys.sort_unstable();
        keys.dedup();
        let index = self
            .db
            .cached_index_handle(self.table.schema.id, &choice.index.name)?;
        let Some(entries) = self
            .db
            .lookup_index_entries_many_keyed_bounded_with_handle(
                &index,
                &self.table.schema,
                &choice.index,
                &keys,
                self.txn,
                choice.max_entries,
                false,
            )?
        else {
            analyze_stats::add_runtime_index_candidates(keys.len() as u64, 0, true);
            return Ok(());
        };
        let probes = keys.len() as u64;
        let examined = entries.len() as u64;
        analyze_stats::add_range_scan_work(entries.len() as u64, 0, 0);
        let mut ids: Vec<_> = entries.iter().map(|(_, id)| *id).collect();
        drop(entries);
        drop(keys);
        ids.sort_unstable();
        ids.dedup();
        let pages = ids
            .iter()
            .map(|id| id.page_id)
            .fold((None, 0u64), |(previous, pages), page| {
                (Some(page), pages + u64::from(previous != Some(page)))
            })
            .1;
        if pages > (self.table.schema.stats.heap_page_count / 2).max(1) {
            analyze_stats::add_runtime_index_candidates(probes, examined, true);
            return Ok(());
        }
        analyze_stats::add_runtime_index_candidates(probes, examined, false);
        let indexed = vec_index_scan::IndexBatchStream::from_row_ids(
            &self.db,
            self.txn,
            &self.table,
            &choice.index,
            ids,
            Some(&self.columns),
        )?;
        drop(workspace);
        let Some(indexed) = indexed else {
            return Ok(());
        };
        self.probes = self
            .filters
            .iter()
            .map(|filter| {
                super::super::super::plan::compile_runtime_filter_probe_projected(
                    filter,
                    &self.table.schema,
                    &self.columns,
                )
            })
            .collect();
        self.adopt_indexed(indexed, pages)?;
        Ok(())
    }
}

impl BatchOperator for RuntimeIndexScan<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn properties(&self) -> BatchProperties {
        if self.indexed.is_some()
            || (!self.opened
                && (self.choice().is_some()
                    || self.intersection_choice().is_some()
                    || self.narrow_choice().is_some()))
        {
            BatchProperties::UNORDERED_SINGLE
        } else {
            self.original.properties()
        }
    }
    fn is_native_parallel_scan(&self) -> bool {
        !self.opened
            && self.choice().is_none()
            && self.intersection_choice().is_none()
            && self.narrow_choice().is_none()
            && self.original.is_native_parallel_scan()
    }
    fn take_parallel_workers(
        &mut self,
        degree: usize,
    ) -> Result<
        Option<Vec<Box<dyn crate::engine::executor::vec::stream::ParallelBatchWorker>>>,
        QueryError,
    > {
        if self.opened
            || self.choice().is_some()
            || self.intersection_choice().is_some()
            || self.narrow_choice().is_some()
        {
            return Ok(None);
        }
        self.original.take_parallel_workers(degree)
    }
    fn prefer_key_locality(&mut self, columns: &[usize]) -> bool {
        self.original.prefer_key_locality(columns)
    }
    fn sample_scan_pages(&mut self, pages: usize) -> bool {
        self.allowed = false;
        self.original.sample_scan_pages(pages)
    }
    fn limit_column_prefixes(&mut self, limits: &[(usize, usize)]) {
        self.allowed = false;
        self.original.limit_column_prefixes(limits);
    }
    fn benefits_from_runtime_key_components(&self, keys: &[Expr]) -> bool {
        if self.opened
            || !self.allowed
            || self.table.schema.is_partitioned()
            || self.table.system_time.is_some()
            || self.txn.historical
            || self.db.has_pending_index_repairs()
        {
            return false;
        }
        keys.iter().any(|key| {
            let name = match key {
                Expr::Identifier(name) => name,
                Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                    let qualifier = &parts[parts.len() - 2];
                    if !self
                        .table
                        .alias
                        .as_deref()
                        .unwrap_or(&self.table.schema.name)
                        .eq_ignore_ascii_case(qualifier)
                    {
                        return false;
                    }
                    &parts[parts.len() - 1]
                }
                _ => return false,
            };
            let Ok(column) = self.table.schema.column_index(name) else {
                return false;
            };
            if !self.columns.contains(&column)
                || !matches!(
                    self.table.schema.columns[column].data_type,
                    crate::storage::DataTypeKind::TinyInt
                        | crate::storage::DataTypeKind::SmallInt
                        | crate::storage::DataTypeKind::MediumInt
                        | crate::storage::DataTypeKind::Int
                        | crate::storage::DataTypeKind::BigInt
                )
            {
                return false;
            }
            self.table.schema.iter_queryable_indexes().any(|index| {
                !index.has_expression_parts()
                    && !index.has_prefix_columns()
                    && !index.has_multi_valued_parts()
                    && !index.is_fulltext()
                    && !index.is_spatial()
                    && !index.is_vector()
                    && index
                        .columns
                        .first()
                        .is_some_and(|first| first.eq_ignore_ascii_case(name))
                    && self
                        .table
                        .schema
                        .index_prefix_cardinality(&index.name, 1)
                        .is_some()
            })
        })
    }
    fn install_runtime_filter(&mut self, filter: RuntimeFilter) -> bool {
        if self.opened || !self.original.install_runtime_filter(filter.clone()) {
            return false;
        }
        self.filters.push(filter);
        true
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if !self.opened {
            self.start()?;
        }
        let Some(indexed) = self.indexed.as_mut() else {
            return self.original.next_batch();
        };
        loop {
            let Some(mut batch) = indexed.next_batch()? else {
                return Ok(None);
            };
            if let Some(residual) = self.residual.as_ref() {
                let mask = residual.eval_predicate_bitmap(&batch)?;
                batch = batch.with_selection(Selection::Mask(mask));
            }
            let selected: Vec<u32> = batch
                .visible_indices()
                .filter(|&row| {
                    super::super::super::plan::runtime_filters_admit_values_indexed(
                        &self.probes,
                        &batch.row(row),
                    )
                })
                .map(|row| row as u32)
                .collect();
            if !selected.is_empty() {
                let batch = batch.with_selection(Selection::Indices(selected));
                // Optional local access can retain private predicate lanes.
                // They never cross the original scan's output boundary.
                return if batch.columns().len() == self.schema.len() {
                    Ok(Some(batch))
                } else {
                    let columns = (0..self.schema.len()).collect::<Vec<_>>();
                    super::super::project_cte_batch(batch, &self.schema, &columns).map(Some)
                };
            }
        }
    }
}

pub(super) fn wrap<'a>(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    columns: Vec<usize>,
    filters: &[RuntimeFilter],
    original: Box<dyn BatchOperator + 'a>,
) -> Box<dyn BatchOperator + 'a> {
    let schema = original.schema().clone();
    Box::new(RuntimeIndexScan {
        db: db.clone(),
        txn,
        table: table.clone(),
        columns,
        schema,
        original,
        filters: filters.to_vec(),
        indexed: None,
        probes: Vec::new(),
        opened: false,
        allowed: true,
        residual: None,
    })
}

/// A local predicate must remain authoritative when optional RID access
/// bypasses its original storage reader. Proven local integer predicates may
/// retain private lanes inside that optional source only.
pub(super) fn wrap_filtered<'a>(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    mut columns: Vec<usize>,
    filters: &[RuntimeFilter],
    original: Box<dyn BatchOperator + 'a>,
    predicate: &Expr,
) -> Result<Box<dyn BatchOperator + 'a>, QueryError> {
    if !crate::planner::expr_is_deterministic(predicate) {
        return Ok(original);
    }
    let schema = original.schema().clone();
    let mut filters = filters.to_vec();
    let local = (!txn.historical && !txn.dirty_read)
        .then(|| local::domains(table, predicate))
        .flatten();
    let residual_schema = if let Some(local) = local {
        let mut private = std::collections::BTreeSet::new();
        if super::super::super::plan::collect_table_column_refs(
            predicate,
            &table.schema,
            &mut private,
        ) {
            for column in private {
                if !columns.contains(&column) {
                    columns.push(column);
                }
            }
            filters.extend(local);
        }
        super::super::template_to_batch_schema(&super::build_scan_template(table, Some(&columns)))
    } else {
        schema.clone()
    };
    let Some(residual) = expr_batch::BatchExprProgram::compile(predicate, &residual_schema)? else {
        return Ok(original);
    };
    Ok(Box::new(RuntimeIndexScan {
        db: db.clone(),
        txn,
        table: table.clone(),
        columns,
        schema,
        original,
        filters,
        indexed: None,
        probes: Vec::new(),
        opened: false,
        allowed: true,
        residual: Some(residual),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn runtime_index_candidates_require_page_advantage_and_workspace() {
        for composite in [false, true] {
            assert!(
                entry_limit(1_000_000, 50_000, 10_000, 10, 1 << 20, composite, false).is_some()
            );
            assert!(
                entry_limit(1_000_000, 50_000, 10_000, 1000, 1 << 20, composite, false).is_none()
            );
            assert!(entry_limit(1_000_000, 0, 10_000, 10, 1 << 20, composite, false).is_none());
            assert!(entry_limit(1_000_000, 50_000, 0, 10, 1 << 20, composite, false).is_none());
            assert!(entry_limit(1_000_000, 50_000, 10_000, 10, 1024, composite, false).is_none());
        }
        assert!(entry_limit(1_000_000, 50_000, 10_000, 400, 1 << 22, false, false).is_none());
        assert!(entry_limit(1_000_000, 50_000, 10_000, 400, 1 << 22, true, false).is_some());
        assert!(entry_limit(57_000_000, 580_000, 26, 1, 256 << 20, true, false).is_none());
        assert!(entry_limit(57_000_000, 580_000, 26, 1, 256 << 20, true, true).is_some());
        assert!(entry_limit(57_000_000, 580_000, 10, 1, 256 << 20, true, true).is_none());
        assert!(entry_limit(57_000_000, 580_000, 26, 1, 1 << 20, true, true).is_none());
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::engine::StorageQueryEngine;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
            self.pulls.fetch_add(1, Ordering::Relaxed);
            self.inner.next_batch()
        }
    }

    #[test]
    fn runtime_index_candidates_preserve_rows_and_bound_stale_statistics() {
        use crate::engine::executor::select::bloom::ExactIntKeySetBuilder;
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        engine
            .execute("CREATE TABLE runtime_rids (k BIGINT, v INT, INDEX by_k(k))")
            .unwrap();
        let values = (0..100)
            .map(|n| format!("({},{n})", n % 10))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!(
                "INSERT INTO runtime_rids VALUES {values},(NULL,100)"
            ))
            .unwrap();
        engine.execute("ANALYZE TABLE runtime_rids").unwrap();
        let db = engine.db_for_tests();
        let original = db.table("runtime_rids").unwrap();
        let index = original
            .indexes
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case("by_k"))
            .unwrap();
        let handle = db.cached_index_handle(original.id, &index.name).unwrap();
        assert!(db.has_pending_index_repairs());
        assert!(db.lookup_index_entries_many_keyed_bounded_with_handle(
            &handle, &original, index, &[crate::storage::IndexKey::from(vec![crate::storage::KeyPart::Int(1)])],
            TxnView::read_only(u64::MAX, crate::storage::IsolationLevel::ReadCommitted), 100, false,
        ).unwrap().is_none(), "a bounded optional lookup must decline pending repairs without first materializing them");
        // Retained committed repair keys deliberately disable this optional
        // path. Model the quiescent read workload after its history horizon.
        db.set_undo_retention_ms(0).unwrap();
        db.gc_committed_txn_slots();
        let budget = Arc::new(mem_budget::MemBudget::new(64 << 20));
        mem_budget::with_query_budget(budget.clone(), || {
            let mut builder = ExactIntKeySetBuilder::with_capacity(2).unwrap();
            builder.insert(1);
            builder.insert(2);
            let filter = RuntimeFilter::ExactIntKey {
                keys: vec![Expr::CompoundIdentifier(vec!["r".into(), "k".into()])],
                null_safe: vec![false],
                filter: Arc::new(builder.finish().unwrap()),
                rejected: Arc::new(std::sync::OnceLock::new()),
            };
            for stale in [false, true] {
                let mut table =
                    PlanTable::new(1, db.table("runtime_rids").unwrap(), Some("r".into()));
                let stats = &mut Arc::make_mut(&mut table.schema).stats;
                stats.row_count = 10_000;
                stats.heap_page_count = 1000;
                stats
                    .indexes
                    .get_mut("by_k")
                    .unwrap()
                    .prefix_distinct_counts = vec![if stale { 10_000 } else { 1000 }];
                let schema = build_scan_batch_schema(&table, &[0, 1]);
                let expected = (0..100)
                    .filter(|n| n % 10 == 1 || n % 10 == 2)
                    .map(|n| vec![Value::Int(n % 10), Value::Int(n)])
                    .collect::<Vec<_>>();
                let batch = Batch::new(
                    schema.clone(),
                    vec![
                        ColumnVector::from_values(expected.iter().map(|r| r[0].clone()).collect()),
                        ColumnVector::from_values(expected.iter().map(|r| r[1].clone()).collect()),
                    ],
                );
                let pulls = Arc::new(AtomicUsize::new(0));
                let fallback = Box::new(Fallback {
                    inner: MaterializedBatchOperator::new(schema, vec![batch]),
                    pulls: pulls.clone(),
                });
                let txn =
                    TxnView::read_only(u64::MAX, crate::storage::IsolationLevel::ReadCommitted);
                let mut source = RuntimeIndexScan {
                    db: db.clone(),
                    txn,
                    table: table.clone(),
                    columns: vec![0, 1],
                    schema: fallback.schema().clone(),
                    original: fallback,
                    filters: Vec::new(),
                    indexed: None,
                    probes: Vec::new(),
                    opened: false,
                    allowed: true,
                    residual: None,
                };
                assert!(
                    source.benefits_from_runtime_key_components(&[Expr::CompoundIdentifier(vec![
                        "r".into(),
                        "k".into()
                    ])])
                );
                assert!(
                    !source.benefits_from_runtime_key_components(&[Expr::CompoundIdentifier(
                        vec!["other".into(), "k".into()]
                    )])
                );
                assert!(source.install_runtime_filter(filter.clone()));
                source.txn.historical = true;
                assert!(
                    source.choice().is_none(),
                    "inherited historical views retain their original reader"
                );
                assert!(
                    !source.benefits_from_runtime_key_components(&[Expr::Identifier("k".into())])
                );
                source.txn.historical = false;
                let choice = source.choice();
                assert!(
                    choice.is_some(),
                    "metadata admission: repairs={} rows={} pages={} ndv={:?}",
                    db.has_pending_index_repairs(),
                    table.schema.stats.row_count,
                    table.schema.stats.heap_page_count,
                    table.schema.index_prefix_cardinality("by_k", 1)
                );
                let choice = choice.unwrap();
                assert!(
                    mem_budget::ControlledMemoryCharge::try_new_optional(
                        mem_budget::ControlledMemoryCategory::Executor,
                        choice.bytes
                    )
                    .is_some(),
                    "optional workspace admission"
                );
                source.start().unwrap();
                assert_eq!(
                    source.indexed.is_some(),
                    !stale,
                    "entries={} bytes={}",
                    choice.max_entries,
                    choice.bytes
                );
                let mut actual = Vec::new();
                while let Some(batch) = source.next_batch().unwrap() {
                    for row in batch.visible_indices() {
                        actual.push(batch.row(row));
                    }
                }
                actual.sort_by_key(|r| match r[1] {
                    Value::Int(v) => v,
                    _ => -1,
                });
                assert_eq!(actual, expected);
                assert_eq!(pulls.load(Ordering::Relaxed) > 0, stale, "bounded index lookup must fall back before output when stats underpredict fanout");
                drop(source);
            }
            for parallel in [false, true] {
                for hint in [None, Some("FULL_SCAN"), Some("NO_INDEX(by_k)")] {
                    let schema = db.table("runtime_rids").unwrap();
                    let mut table = PlanTable::new(schema.id, schema, Some("r".into()));
                    let stats = &mut Arc::make_mut(&mut table.schema).stats;
                    stats.row_count = 10_000;
                    stats.heap_page_count = 1000;
                    stats
                        .indexes
                        .get_mut("by_k")
                        .unwrap()
                        .prefix_distinct_counts = vec![1000];
                    let plan = if parallel {
                        crate::planner::PhysicalPlan::ParallelSeqScan {
                            table,
                            filter: None,
                            projected_columns: None,
                            runtime_filters: vec![filter.clone()],
                            estimated_rows: 10_000.0,
                            access_rows: 10_000.0,
                            possible_keys: vec![],
                            cost: crate::engine::cbo::Cost::scalar(1000.0),
                            hint_note: hint.map(str::to_owned),
                            degree: 2,
                            output_columns_in_projection: None,
                        }
                    } else {
                        crate::planner::PhysicalPlan::SeqScan {
                            table,
                            filter: None,
                            projected_columns: None,
                            runtime_filters: vec![filter.clone()],
                            estimated_rows: 10_000.0,
                            access_rows: 10_000.0,
                            possible_keys: vec![],
                            cost: crate::engine::cbo::Cost::scalar(1000.0),
                            hint_note: hint.map(str::to_owned),
                        }
                    };
                    let txn =
                        TxnView::read_only(u64::MAX, crate::storage::IsolationLevel::ReadCommitted);
                    let mut input = try_build_direct_batch_scan(db, txn, &plan)
                        .unwrap()
                        .unwrap();
                    assert_eq!(input.stream.benefits_from_runtime_key_components(&[Expr::CompoundIdentifier(vec!["r".into(),"k".into()])]), hint.is_none(),
                        "explicit access hints must not acquire optional index candidates: {hint:?} parallel={parallel}");
                    let mut actual = Vec::new();
                    while let Some(batch) = input.stream.next_batch().unwrap() {
                        actual.extend(batch.visible_indices().map(|row| batch.row(row)));
                    }
                    actual.sort_by_key(|row| match row[1] {
                        Value::Int(n) => n,
                        _ => panic!(),
                    });
                    let expected = (0..100)
                        .filter(|n| n % 10 == 1 || n % 10 == 2)
                        .map(|n| vec![Value::Int(n % 10), Value::Int(n)])
                        .collect::<Vec<_>>();
                    assert_eq!(actual, expected);
                    drop(input);
                }
            }
            drop(filter);
            assert_eq!(budget.used(), 0);
        });
    }

    #[test]
    fn runtime_index_checks_actual_heap_page_coverage_before_output() {
        use crate::engine::executor::select::bloom::ExactIntKeySetBuilder;
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        let columns = (0..200)
            .map(|n| format!("v{n} BIGINT"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!(
                "CREATE TABLE runtime_pages(k BIGINT,{columns},INDEX by_k(k))"
            ))
            .unwrap();
        let values = (0..100)
            .map(|n| {
                let mut row = vec![(n % 10).to_string()];
                row.extend(std::iter::repeat_n(n.to_string(), 200));
                format!("({})", row.join(","))
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO runtime_pages VALUES {values}"))
            .unwrap();
        engine.execute("ANALYZE TABLE runtime_pages").unwrap();
        let db = engine.db_for_tests();
        db.set_undo_retention_ms(0).unwrap();
        db.gc_committed_txn_slots();
        let budget = Arc::new(mem_budget::MemBudget::new(64 << 20));
        mem_budget::with_query_budget(budget.clone(), || {
            let mut builder = ExactIntKeySetBuilder::with_capacity(2).unwrap();
            builder.insert(1);
            builder.insert(2);
            let filter = RuntimeFilter::ExactIntKey {
                keys: vec![Expr::Identifier("k".into())],
                null_safe: vec![false],
                filter: Arc::new(builder.finish().unwrap()),
                rejected: Arc::new(std::sync::OnceLock::new()),
            };
            let mut tuple = crate::engine::executor::select::bloom::BloomFilter::new_int_pair(20);
            for n in (0..100).filter(|n| n % 10 == 1 || n % 10 == 2) {
                tuple.insert_int_pair(i128::from(n % 10), i128::from(n));
            }
            let tuple_filter = RuntimeFilter::BloomKey {
                keys: vec![Expr::Identifier("k".into()), Expr::Identifier("v0".into())],
                null_safe: vec![false, false],
                filter: Arc::new(tuple),
                rejected: None,
            };
            for (pages, indexed) in [(10, false), (1000, true)] {
                let schema = db.table("runtime_pages").unwrap();
                let mut table = PlanTable::new(schema.id, schema, None);
                let stats = &mut Arc::make_mut(&mut table.schema).stats;
                stats.row_count = 10_000;
                stats.heap_page_count = pages;
                stats
                    .indexes
                    .get_mut("by_k")
                    .unwrap()
                    .prefix_distinct_counts = vec![2000];
                let schema = build_scan_batch_schema(&table, &[0, 1]);
                let expected = (0..100)
                    .filter(|n| n % 10 == 1 || n % 10 == 2)
                    .map(|n| vec![Value::Int(n % 10), Value::Int(n)])
                    .collect::<Vec<_>>();
                let batch = Batch::new(
                    schema.clone(),
                    vec![
                        ColumnVector::from_values(
                            expected.iter().map(|row| row[0].clone()).collect(),
                        ),
                        ColumnVector::from_values(
                            expected.iter().map(|row| row[1].clone()).collect(),
                        ),
                    ],
                );
                let pulls = Arc::new(AtomicUsize::new(0));
                let fallback = Box::new(Fallback {
                    inner: MaterializedBatchOperator::new(schema.clone(), vec![batch]),
                    pulls: pulls.clone(),
                });
                let mut source = RuntimeIndexScan {
                    db: db.clone(),
                    txn: TxnView::read_only(
                        u64::MAX,
                        crate::storage::IsolationLevel::ReadCommitted,
                    ),
                    table,
                    columns: vec![0, 1],
                    schema,
                    original: fallback,
                    filters: vec![filter.clone()],
                    indexed: None,
                    probes: Vec::new(),
                    opened: false,
                    allowed: true,
                    residual: None,
                };
                if pages == 10 {
                    assert!(
                        source.choice().is_none(),
                        "cheap single-key scans retain the stronger page advantage"
                    );
                    mem_budget::with_test_effective_memory_limit(crate::storage::PAGE_SIZE, || {
                        assert!(
                            source.choice().is_some(),
                            "a heap exceeding effective host memory can discover bounded locality"
                        );
                    });
                    let mut other = ExactIntKeySetBuilder::with_capacity(1).unwrap();
                    other.insert(1);
                    source.filters.push(RuntimeFilter::ExactIntKey {
                        keys: vec![Expr::Identifier("v0".into())],
                        null_safe: vec![false],
                        filter: Arc::new(other.finish().unwrap()),
                        rejected: Arc::new(std::sync::OnceLock::new()),
                    });
                    mem_budget::with_test_effective_memory_limit(crate::storage::PAGE_SIZE, || {
                        assert!(source.choice().is_none(), "selective peer domains keep native predicate-first decoding even on a large heap");
                    });
                    source.filters.pop();
                }
                source.filters.push(tuple_filter.clone());
                assert!(source.choice().is_some(), "bounded candidate admission");
                source.start().unwrap();
                assert_eq!(source.indexed.is_some(), indexed);
                let mut actual = Vec::new();
                while let Some(batch) = source.next_batch().unwrap() {
                    actual.extend(batch.visible_indices().map(|row| batch.row(row)));
                }
                let key = |v: &Vec<Value>| match &v[1] {
                    Value::Int(n) => *n,
                    _ => panic!(),
                };
                actual.sort_by_key(key);
                let mut expected = expected;
                expected.sort_by_key(key);
                assert_eq!(actual, expected);
                assert_eq!(pulls.load(Ordering::Relaxed) > 0, !indexed);
                drop(source);
            }
            drop(tuple_filter);
            drop(filter);
            assert_eq!(budget.used(), 0);
        });
    }
}

#[cfg(test)]
mod residual_tests {
    use super::*;
    use crate::engine::{executor::select::bloom::ExactIntKeySetBuilder, StorageQueryEngine};
    struct Original {
        schema: Arc<BatchSchema>,
        pulls: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl BatchOperator for Original {
        fn schema(&self) -> &Arc<BatchSchema> {
            &self.schema
        }
        fn install_runtime_filter(&mut self, _: RuntimeFilter) -> bool {
            true
        }
        fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
            self.pulls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(None)
        }
    }
    #[test]
    fn filtered_rid_candidates_recheck_local_predicate_and_decline_private_columns() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        engine
            .execute("CREATE TABLE filtered_rids(k BIGINT,v BIGINT,INDEX by_k(k))")
            .unwrap();
        engine
            .execute("INSERT INTO filtered_rids SELECT MOD(n,10),n FROM generate_series(0,99) g(n)")
            .unwrap();
        engine
            .execute("INSERT INTO filtered_rids VALUES(1,NULL),(NULL,91)")
            .unwrap();
        engine.execute("ANALYZE TABLE filtered_rids").unwrap();
        let db = engine.db_for_tests();
        db.set_undo_retention_ms(0).unwrap();
        db.gc_committed_txn_slots();
        let budget = Arc::new(mem_budget::MemBudget::new(64 << 20));
        mem_budget::with_query_budget(budget.clone(), || {
            for columns in [vec![0, 1], vec![0]] {
                let mut table = PlanTable::new(1, db.table("filtered_rids").unwrap(), None);
                let stats = &mut Arc::make_mut(&mut table.schema).stats;
                stats.row_count = 10_000;
                stats.heap_page_count = 1000;
                stats
                    .indexes
                    .get_mut("by_k")
                    .unwrap()
                    .prefix_distinct_counts = vec![1000];
                let schema = build_scan_batch_schema(&table, &columns);
                let pulls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let original = Box::new(Original {
                    schema,
                    pulls: pulls.clone(),
                });
                let crate::parser::Statement::Select(query) = crate::parser::parse_statement(
                    "SELECT k FROM filtered_rids WHERE v>=50 AND k IS NOT NULL",
                )
                .unwrap() else {
                    panic!()
                };
                let txn = TxnView::read_only(
                    db.visible_scn(),
                    crate::storage::IsolationLevel::ReadCommitted,
                );
                let mut stream = wrap_filtered(
                    &db,
                    txn,
                    &table,
                    columns.clone(),
                    &[],
                    original,
                    query.selection.as_ref().unwrap(),
                )
                .unwrap();
                let mut keys = ExactIntKeySetBuilder::with_capacity(2).unwrap();
                keys.insert(1);
                keys.insert(2);
                assert!(stream.install_runtime_filter(RuntimeFilter::ExactIntKey {
                    keys: vec![Expr::Identifier("k".into())],
                    null_safe: vec![false],
                    filter: Arc::new(keys.finish().unwrap()),
                    rejected: Arc::new(std::sync::OnceLock::new())
                }));
                let mut rows = Vec::new();
                while let Some(batch) = stream.next_batch().unwrap() {
                    rows.extend(batch.visible_indices().map(|r| batch.row(r)));
                }
                if columns.len() == 2 {
                    rows.sort_by_key(|r| match r[1] {
                        Value::Int(n) => n,
                        _ => panic!(),
                    });
                    let expected = (50..100)
                        .filter(|n| n % 10 == 1 || n % 10 == 2)
                        .map(|n| vec![Value::Int(n % 10), Value::Int(n)])
                        .collect::<Vec<_>>();
                    assert_eq!(rows, expected);
                    assert_eq!(pulls.load(std::sync::atomic::Ordering::Relaxed), 0);
                } else {
                    assert_eq!(
                        pulls.load(std::sync::atomic::Ordering::Relaxed),
                        1,
                        "private predicate columns retain their reader"
                    );
                }
            }
        });
        assert_eq!(budget.used(), 0);
    }
}
