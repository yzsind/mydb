//! Adaptive filtered KNN planning in the same cost units as relational access.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilteredVectorStrategy {
    Exact,
    PrefilterAnn,
    IterativeAnn,
}
impl FilteredVectorStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::PrefilterAnn => "prefilter_ann",
            Self::IterativeAnn => "iterative_ann",
        }
    }
}

/// Unique traversal work shared by all batches of a retained cursor. A
/// larger scalar candidate demand may widen exploration, but never repeats
/// the whole base traversal once per rejected candidate.
fn retained_navigation_rows(
    index: &IndexSchema,
    population: f64,
    effort: usize,
    pool: f64,
    survival: f64,
) -> f64 {
    match index.vector_algo() {
        crate::storage::schema::VectorAlgo::Hnsw => {
            let effort = (effort as f64).max(pool / survival);
            (effort * (index.vector_meta().m as f64 + population.max(2.0).ln())).min(population)
        }
        crate::storage::schema::VectorAlgo::Ivf => {
            let lists = index
                .vector_meta()
                .nlist
                .map(f64::from)
                .unwrap_or_else(|| population.sqrt())
                .max(1.0);
            let needed = pool / (population / lists * survival).max(1.0 / population);
            lists + population * ((effort as f64).max(needed) / lists).min(1.0)
        }
    }
}

#[cfg(test)]
mod retained_cursor_cost_tests {
    use super::*;

    #[test]
    fn filtering_does_not_repeat_an_already_sufficient_cursor_traversal() {
        let mut index: IndexSchema = serde_json::from_value(serde_json::json!({
            "name": "vx", "columns": ["v"], "unique": false,
            "primary": false, "index_type": "HNSW"
        }))
        .unwrap();
        for algorithm in ["HNSW", "IVF"] {
            index.index_type = algorithm.into();
            let all = retained_navigation_rows(&index, 100_000.0, 256, 80.0, 1.0);
            let half = retained_navigation_rows(&index, 100_000.0, 256, 80.0, 0.5);
            assert_eq!(all, half, "a retained {algorithm} cursor shares base work");
            let sparse = retained_navigation_rows(&index, 100_000.0, 256, 80.0, 0.0001);
            assert!(
                sparse > half,
                "insufficient expected matches require more work"
            );
            assert!(sparse.is_finite());
        }
    }
}

/// Remaining work after scalar filtering, in the ordinary CBO cost units.
#[allow(clippy::too_many_arguments)]
pub(crate) fn filtered_vector_remaining_cost(
    db: &Database,
    table: &PlanTable,
    index: &IndexSchema,
    dim: u32,
    survivors: usize,
    k: usize,
    effort: usize,
) -> Result<(f64, f64), QueryError> {
    use crate::engine::cbo::{operator_cost, CboContext};
    let population = table_row_estimate(db, table)?.max(1.0);
    let survivors = survivors as f64;
    let context = CboContext::from_db(db);
    let distance =
        (f64::from(dim) / 8.0).max(1.0) * crate::engine::cbo::cbo_consts().cpu_operator_cost();
    let heap_pages = if table.schema.stats.heap_page_count > 0 {
        table.schema.stats.heap_page_count as f64
    } else {
        operator_cost::estimated_heap_pages(
            population,
            operator_cost::estimated_row_width(&table.schema),
        )
    };
    let heap_work = |rows| {
        operator_cost::index_lookup(&context, 0.0, rows, heap_pages, 0.0).total + rows * distance
    };
    let cfg = crate::engine::vector::session::current_config();
    let pool = k
        .saturating_mul(crate::engine::vector::session::effective_rerank_mult(&cfg, index) as usize)
        .max(32)
        .min(survivors as usize) as f64;
    let navigation_rows = retained_navigation_rows(
        index,
        population,
        effort,
        pool,
        (survivors / population).clamp(1.0 / population, 1.0),
    );
    let column = table.schema.column_index(&index.columns[0])?;
    let payload = match table.schema.columns[column].data_type {
        crate::storage::DataTypeKind::Vector {
            datatype,
            dim: Some(dim),
        } => datatype.payload_len(dim) as f64,
        _ => 16.0,
    };
    let graph_pages = operator_cost::estimated_heap_pages(
        population,
        payload + index.vector_meta().m as f64 * 8.0 + 32.0,
    );
    let navigation = operator_cost::index_lookup(&context, 0.0, navigation_rows, graph_pages, 0.0)
        .total
        + navigation_rows * distance;
    Ok((heap_work(survivors), navigation + heap_work(pool)))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn plan_filtered_vector(
    db: &Database,
    table: &PlanTable,
    select: &Select,
    filter: Option<Expr>,
    possible_keys: Vec<String>,
    col: &str,
    query_vector: crate::vector::VectorValue,
    query_vector_expr: Option<Expr>,
    metric: crate::engine::vector::Metric,
    k: usize,
    offset: usize,
    total_rows: f64,
    filtered_rows: f64,
    index: Option<&IndexSchema>,
    hint: Option<u64>,
) -> Option<PhysicalPlan> {
    if !vector_brute_scan_enabled() || k > topk_max_heap_from_env() {
        return None;
    }
    let predicate = filter.as_ref()?;
    // Relocating opaque/volatile SQL can change effects and evaluation count.
    if !expr_is_deterministic(predicate) || expr_has_volatile_evaluation(predicate) {
        return None;
    }
    let column_index = table.schema.column_index(col).ok()?;
    let crate::storage::DataTypeKind::Vector { dim: Some(dim), .. } =
        table.schema.columns[column_index].data_type
    else {
        return None;
    };
    // NULL/zero-cosine rankings use the relational oracle, which has the full
    // SQL NULL placement contract rather than the ANN non-null contract.
    if table.schema.columns[column_index].nullable
        || (metric == crate::engine::vector::Metric::Cosine && index.is_none())
        || select.order_by.len() != 1
    {
        return None;
    }
    let mut required = HashSet::new();
    collect_expr_columns(predicate, &mut required);
    let name = table.alias.as_deref().unwrap_or(&table.schema.name);
    let hints = ScanHints {
        required_columns: Some(required),
        index_hints: select
            .from
            .first()
            .map(|t| t.index_hints.as_slice())
            .unwrap_or_default(),
        query_hints: &select.query_hints,
        table_name: name,
        ..ScanHints::none()
    };
    let scalar_access = choose_scan(db, table, filter.clone(), &hints).ok()?;
    // This leaf owns its own bounded RowId source. Unsupported scalar access
    // shapes remain on the ordinary relational path.
    if !matches!(
        scalar_access,
        PhysicalPlan::SeqScan { .. }
            | PhysicalPlan::ZoneMapScan { .. }
            | PhysicalPlan::IndexLookup { .. }
            | PhysicalPlan::IndexMultiLookup { .. }
            | PhysicalPlan::IndexRangeScan { .. }
            | PhysicalPlan::IndexFastScan { .. }
            | PhysicalPlan::IndexMerge { .. }
    ) {
        return None;
    }
    let cfg = crate::engine::vector::session::current_config();
    let ef = index.map_or(0, |idx| match idx.vector_algo() {
        crate::storage::schema::VectorAlgo::Hnsw => {
            crate::engine::vector::session::effective_ef_search(
                &cfg,
                hint,
                idx.vector_meta().default_ef_search.map(u64::from),
                k,
            )
        }
        crate::storage::schema::VectorAlgo::Ivf => {
            crate::engine::vector::session::effective_nprobe(
                &cfg,
                hint,
                idx.vector_meta().default_nprobe.map(u64::from),
            )
        }
    });
    let scalar = plan_cost(&scalar_access);
    let consts = crate::engine::cbo::cbo_consts();
    let distance = (f64::from(dim) / 8.0).max(1.0) * consts.cpu_operator_cost();
    let survivors = filtered_rows.clamp(0.0, total_rows.max(0.0));
    let context = crate::engine::cbo::CboContext::from_db(db);
    let heap_pages = if table.schema.stats.heap_page_count > 0 {
        table.schema.stats.heap_page_count as f64
    } else {
        crate::engine::cbo::operator_cost::estimated_heap_pages(
            total_rows,
            crate::engine::cbo::operator_cost::estimated_row_width(&table.schema),
        )
    };
    let heap_fetch = |rows| {
        crate::engine::cbo::operator_cost::index_lookup(&context, 0.0, rows, heap_pages, 0.0)
    };
    let exact_fetch = heap_fetch(survivors);
    let exact_work = exact_fetch.total + survivors * distance;
    let exact = scalar.total + exact_work;
    let pool = index.map_or(k, |idx| {
        k.saturating_mul(crate::engine::vector::session::effective_rerank_mult(&cfg, idx) as usize)
            .max(32)
    });
    let survival = (survivors / total_rows.max(1.0)).clamp(1.0 / total_rows.max(1.0), 1.0);
    let widening = index.is_none_or(|idx| {
        if idx.vector_algo() == crate::storage::schema::VectorAlgo::Ivf {
            cfg.ivf_iterative_scan != crate::engine::vector::session::IterativeScan::Off
        } else {
            cfg.iterative_scan != crate::engine::vector::session::IterativeScan::Off
        }
    });
    let navigation_rows = index.map_or(0.0, |idx| {
        retained_navigation_rows(
            idx,
            total_rows.max(1.0),
            ef,
            pool as f64,
            if widening { survival } else { 1.0 },
        )
    });
    let node_width = table.schema.columns[column_index].data_type.clone();
    let payload = match node_width {
        crate::storage::DataTypeKind::Vector {
            datatype,
            dim: Some(dim),
        } => datatype.payload_len(dim) as f64,
        _ => 16.0,
    };
    let graph_pages = crate::engine::cbo::operator_cost::estimated_heap_pages(
        total_rows,
        payload + index.map_or(0.0, |i| i.vector_meta().m as f64 * 8.0) + 32.0,
    );
    let navigation_fetch = crate::engine::cbo::operator_cost::index_lookup(
        &context,
        0.0,
        navigation_rows,
        graph_pages,
        0.0,
    );
    let navigation = if index.is_none() {
        f64::INFINITY
    } else {
        navigation_fetch.total + navigation_rows * distance
    };
    let prefilter_navigation_rows = index.map_or(0.0, |idx| {
        retained_navigation_rows(idx, total_rows.max(1.0), ef, pool as f64, survival)
    });
    let prefilter_navigation_fetch = crate::engine::cbo::operator_cost::index_lookup(
        &context,
        0.0,
        prefilter_navigation_rows,
        graph_pages,
        0.0,
    );
    let prefilter_navigation = if index.is_none() {
        f64::INFINITY
    } else {
        prefilter_navigation_fetch.total + prefilter_navigation_rows * distance
    };
    let pool_rows = (pool as f64).min(survivors);
    let pool_fetch = heap_fetch(pool_rows);
    let pool_work = pool_fetch.total + pool_rows * distance;
    let prefilter = scalar.total + prefilter_navigation + pool_work;
    let candidates = (pool as f64 / if widening { survival } else { 1.0 }).min(total_rows);
    let candidate_fetch = heap_fetch(candidates);
    let iterative =
        navigation + candidate_fetch.total + candidates * qual_cost_per_row(predicate) + pool_work;
    // Complex residuals have weak selectivity estimates. Price a bounded
    // vector-first pilot as well; execution observes its survival before
    // spending more work or completing through the scalar source.
    let pilot =
        navigation + pool_fetch.total + pool as f64 * qual_cost_per_row(predicate) + pool_work;
    let probe_complex_filter = index.is_some()
        && widening
        && qual_cost_per_row(predicate) >= distance * 8.0
        && matches!(
            scalar_access,
            PhysicalPlan::SeqScan { .. } | PhysicalPlan::ZoneMapScan { .. }
        )
        && pilot < exact.min(prefilter);
    let (strategy, total) = if probe_complex_filter {
        (FilteredVectorStrategy::IterativeAnn, iterative)
    } else if index.is_none()
        || (hint.is_none() && cfg.brute_threshold != 0 && exact <= prefilter.min(iterative))
    {
        (FilteredVectorStrategy::Exact, exact)
    } else if prefilter <= iterative {
        (FilteredVectorStrategy::PrefilterAnn, prefilter)
    } else {
        (FilteredVectorStrategy::IterativeAnn, iterative)
    };
    let mut cost = if strategy == FilteredVectorStrategy::IterativeAnn {
        crate::engine::cbo::Cost::default()
    } else {
        scalar
    };
    let (cpu, random_pages) = match strategy {
        FilteredVectorStrategy::Exact => (
            exact_fetch.cpu_units + survivors * distance,
            exact_fetch.io_rnd_pages,
        ),
        FilteredVectorStrategy::PrefilterAnn => (
            prefilter_navigation_fetch.cpu_units
                + prefilter_navigation_rows * distance
                + pool_fetch.cpu_units
                + pool_rows * distance,
            prefilter_navigation_fetch.io_rnd_pages + pool_fetch.io_rnd_pages,
        ),
        FilteredVectorStrategy::IterativeAnn => (
            navigation_fetch.cpu_units
                + navigation_rows * distance
                + candidate_fetch.cpu_units
                + candidates * qual_cost_per_row(predicate)
                + pool_fetch.cpu_units
                + pool_rows * distance,
            navigation_fetch.io_rnd_pages + candidate_fetch.io_rnd_pages + pool_fetch.io_rnd_pages,
        ),
    };
    cost.cpu_units += cpu;
    cost.io_rnd_pages += random_pages;
    cost.total = total;
    cost.mem_bytes += (k as f64 * 32.0)
        + if strategy == FilteredVectorStrategy::PrefilterAnn {
            survivors * 8.0
        } else {
            pool as f64 * 64.0
        };
    let requested = select
        .query_hints
        .iter()
        .filter_map(|hint| match hint {
            crate::parser::QueryHint::Parallel { degree } => Some(*degree),
            _ => None,
        })
        .last();
    let suppressed = parallel_planner_config().disabled
        || parallelism_force_disabled()
        || select
            .query_hints
            .iter()
            .any(|hint| matches!(hint, crate::parser::QueryHint::NoParallel));
    let degree = if suppressed {
        1
    } else {
        cpu_bound_parallel_degree(
            requested
                .filter(|degree| *degree > 0)
                .unwrap_or_else(default_parallel_degree),
        )
        .max(1) as usize
    };
    let score_work = exact_work;
    let parallel_degree = if strategy == FilteredVectorStrategy::Exact
        && degree > 1
        && (requested.is_some()
            || score_work / degree as f64 + degree as f64 * consts.parallel_setup_cost()
                < score_work)
    {
        degree
    } else {
        1
    };
    if parallel_degree > 1 {
        cost.total = scalar.total
            + score_work / parallel_degree as f64
            + parallel_degree as f64 * consts.parallel_setup_cost();
        cost.mem_bytes += survivors * 8.0 + parallel_degree as f64 * k as f64 * 32.0;
    }
    cost.startup = cost.total;
    let mut planned_table = table.clone();
    planned_table.partition_pruning = build_partition_pruning_plan(&table.schema, filter.as_ref());
    Some(PhysicalPlan::FilteredVectorTopK {
        table: planned_table,
        column_index,
        index: index.cloned(),
        scalar_access: Box::new(scalar_access),
        strategy,
        parallel_degree,
        exact_cost: exact,
        predicate_cost: qual_cost_per_row(predicate),
        query_vector,
        query_vector_expr,
        metric: metric.as_str().to_string(),
        k,
        baked_offset: offset,
        binding: limit_binding_of(select),
        ef_search: ef,
        ef_hint: hint,
        order_by: select.order_by.clone(),
        filter,
        estimated_rows: (k as f64).min(survivors),
        access_rows: total_rows,
        cost,
        possible_keys,
        projected_columns: None,
    })
}
