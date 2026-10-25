//! Scoped CTE/derived preparation and analyzed source ownership.
use super::*;

pub(in crate::engine::executor) fn execute_response(
    db: &Arc<Database>,
    txn: TxnView,
    rrf: &RrfFusion,
    _registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let _scoring = super::super::fulltext_context::ensure();
    for branch in &rrf.branches {
        crate::parser::rrf::validate_branch(&branch.query)
            .map_err(|error| error.spec.error(error.message))?;
        reject_unproven_routines(db, &branch.query)?;
    }
    let output_columns = describe_columns(db, rrf)?;
    let (_, weights) = bind_options(rrf)?;
    if current_output_window().is_some_and(|window| {
        super::super::limit_value_or_zero(Some(&window.limit)).is_ok_and(|limit| limit == 0)
    }) {
        plan_for_explain(db, rrf)?;
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns: output_columns,
            rows: Vec::new(),
        }));
    }
    let mut spec = rrf.clone();
    let mut spools = Spools::new();
    for (i, branch) in spec.branches.iter_mut().enumerate() {
        crate::engine::executor::cte::with_cte_registry_extended(spools.clone(), || {
            prepare_source(
                db,
                txn,
                &mut branch.query,
                _registry,
                &mut spools,
                weights[i] > 0.0,
            )
        })?;
    }
    crate::engine::executor::cte::with_cte_registry_extended(spools, || {
        let plan = crate::planner::plan_query_physical(db, crate::parser::QueryRef::Rrf(&spec))?
            .ok_or_else(|| {
                crate::error::catalog::ER_NOT_SUPPORTED_YET.error("RRF branch could not be planned")
            })?;
        let context = super::super::execution_context::ExecutionContext::capture(txn);
        if let Some(result) = super::super::vec_bridge::try_stream_native_plan(
            db.clone(),
            txn,
            Arc::new(plan.clone()),
            context,
        )? {
            return Ok(QueryResponse::StreamingResultSet(result));
        }
        let (columns, rows) =
            super::super::plan::execute_plan_to_columns_and_values(db, txn, &plan)?;
        Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
    })
}

pub(super) fn with_branch_scope<R>(
    db: &Arc<Database>,
    txn: TxnView,
    query: &QueryExpr,
    body: impl FnOnce(&Select) -> Result<R, QueryError>,
) -> Result<R, QueryError> {
    match query {
        QueryExpr::Select(select) => body(select),
        QueryExpr::WithCte(with) => {
            let registry =
                super::super::current_session_registry().unwrap_or_else(SessionRegistry::new);
            let results = crate::engine::executor::cte::materialize_ctes_in_view(
                db, txn, &with.ctes, &registry,
            )?;
            crate::engine::executor::cte::with_cte_registry_extended(results, || {
                with_branch_scope(db, txn, &with.body, body)
            })
        }
        _ => Err(crate::error::catalog::ER_NOT_SUPPORTED_YET.error("unsupported RRF branch")),
    }
}

fn rename_cte_inputs(query: &mut QueryExpr, names: &std::collections::HashMap<String, String>) {
    struct Rename<'a> {
        names: &'a std::collections::HashMap<String, String>,
        shadows: Vec<std::collections::HashSet<String>>,
    }
    impl crate::parser::visit::AstVisitorMut for Rename<'_> {
        fn enter_query(&mut self, query: &mut QueryExpr) {
            if let QueryExpr::WithCte(with) = query {
                let mut shadow = self.shadows.last().cloned().unwrap_or_default();
                shadow.extend(with.ctes.iter().map(|cte| cte.name.to_ascii_lowercase()));
                self.shadows.push(shadow);
            }
        }
        fn exit_query(&mut self, query: &mut QueryExpr) {
            if matches!(query, QueryExpr::WithCte(_)) {
                self.shadows.pop();
            }
        }
        fn enter_select(&mut self, select: &mut Select) {
            let rename = |table: &mut crate::parser::TableRef| {
                let key = table.name.to_ascii_lowercase();
                if self
                    .shadows
                    .last()
                    .is_some_and(|scope| scope.contains(&key))
                {
                    return;
                }
                if let Some(name) = self.names.get(&key) {
                    table.alias.get_or_insert_with(|| table.name.clone());
                    table.name = name.clone();
                }
            };
            for (i, table) in select.from.iter_mut().enumerate() {
                if i != 0 || select.derived.is_none() {
                    rename(table);
                }
            }
            for join in &mut select.joins {
                if join.derived.is_none() {
                    rename(&mut join.table);
                }
            }
            select.table = select.from.first().map(|table| table.name.clone());
        }
    }
    crate::parser::visit::visit_query_mut(
        &mut Rename {
            names,
            shadows: Vec::new(),
        },
        query,
    );
}

type Spools = std::collections::HashMap<String, Arc<crate::engine::api::CteResult>>;

fn prepare_source(
    db: &Arc<Database>,
    txn: TxnView,
    query: &mut QueryExpr,
    registry: &SessionRegistry,
    spools: &mut Spools,
    execute: bool,
) -> Result<(), QueryError> {
    if let QueryExpr::WithCte(with) = query {
        let results = if execute {
            crate::engine::executor::cte::materialize_ctes_in_view(db, txn, &with.ctes, registry)?
        } else {
            crate::engine::executor::cte::describe_definitions(&with.ctes, |query| {
                super::super::describe_subquery_columns(db, query)
            })?
        };
        let mut renamed = std::collections::HashMap::new();
        for (name, result) in results {
            let internal = crate::engine::executor::cte::next_inline_derived_name();
            renamed.insert(name, internal.clone());
            spools.insert(internal, result);
        }
        let mut body = std::mem::replace(&mut with.body, Select::default().into());
        rename_cte_inputs(&mut body, &renamed);
        *query = body;
    }
    let QueryExpr::Select(select) = query else {
        return Err(invalid("expected SELECT retrieval input"));
    };
    fn materialize(
        db: &Arc<Database>,
        txn: TxnView,
        derived: &QueryExpr,
        registry: &SessionRegistry,
        execute: bool,
    ) -> Result<Arc<crate::engine::api::CteResult>, QueryError> {
        if execute {
            let response = crate::engine::executor::dispatch::execute_subquery_response(
                db, txn, derived, registry,
            )?;
            Ok(Arc::new(super::super::materialize_response_with_spill(
                response,
                db.data_dir().to_path_buf(),
            )?))
        } else {
            Ok(Arc::new(crate::engine::api::CteResult::materialized(
                super::super::describe_subquery_columns(db, derived)?,
                Vec::new(),
            )))
        }
    }
    if let Some(derived) = select.derived.take() {
        let result =
            crate::engine::executor::cte::with_cte_registry_extended(spools.clone(), || {
                materialize(db, txn, &derived, registry, execute)
            })?;
        let table = select
            .from
            .first_mut()
            .ok_or_else(|| invalid("derived input has no relation"))?;
        let name = crate::engine::executor::cte::next_inline_derived_name();
        table.alias.get_or_insert_with(|| table.name.clone());
        table.name = name.clone();
        spools.insert(name, result);
        select.table = select.from.first().map(|table| table.name.clone());
    }
    for join in &mut select.joins {
        if let Some(derived) = join.derived.take() {
            if join.lateral {
                return Err(crate::error::catalog::ER_NOT_SUPPORTED_YET
                    .error("RRF does not support correlated LATERAL inputs"));
            }
            let result =
                crate::engine::executor::cte::with_cte_registry_extended(spools.clone(), || {
                    materialize(db, txn, &derived, registry, execute)
                })?;
            let name = crate::engine::executor::cte::next_inline_derived_name();
            join.table
                .alias
                .get_or_insert_with(|| join.table.name.clone());
            join.table.name = name.clone();
            spools.insert(name, result);
        }
    }
    Ok(())
}

pub(in crate::engine::executor::select) fn plan_for_explain(
    db: &Arc<Database>,
    rrf: &RrfFusion,
) -> Result<PhysicalPlan, QueryError> {
    let mut spec = rrf.clone();
    let mut spools = Spools::new();
    let registry = super::super::current_session_registry().unwrap_or_else(SessionRegistry::new);
    for branch in &mut spec.branches {
        crate::engine::executor::cte::with_cte_registry_extended(spools.clone(), || {
            prepare_source(
                db,
                TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted),
                &mut branch.query,
                &registry,
                &mut spools,
                false,
            )
        })?;
    }
    crate::engine::executor::cte::with_cte_registry_extended(spools, || {
        crate::planner::plan_query_physical(db, crate::parser::QueryRef::Rrf(&spec))?
            .ok_or_else(|| invalid("could not plan fusion"))
    })
}

fn reject_unproven_routines(db: &Database, query: &QueryExpr) -> Result<(), QueryError> {
    if !db.has_stored_functions() {
        return Ok(());
    }
    struct Check<'a> {
        db: &'a Database,
        database: String,
        found: bool,
    }
    impl crate::parser::visit::AstVisitor for Check<'_> {
        fn enter_expr(&mut self, expr: &Expr) {
            if let Expr::Function { name, .. } = expr {
                if (name.contains('.')
                    || !crate::engine::cbo::function_cost::is_known_builtin_function(name))
                    && self
                        .db
                        .routine_definition(
                            &self.database,
                            crate::storage::RoutineKindSchema::Function,
                            name,
                        )
                        .is_some()
                {
                    self.found = true;
                }
            }
        }
    }
    let database = crate::engine::expr::current_session_context_snapshot()
        .map(|session| session.current_database)
        .unwrap_or_else(|| crate::storage::DEFAULT_DATABASE.into());
    let mut check = Check {
        db,
        database,
        found: false,
    };
    crate::parser::visit::visit_query(&mut check, query);
    if check.found {
        Err(crate::error::catalog::ER_NOT_SUPPORTED_YET
            .error("RRF stored-function inputs require a proven side-effect-free body"))
    } else {
        Ok(())
    }
}

/// Populate the immutable display graph's spool identities with real query
/// data, retaining the same node tree for ANALYZE rather than executing a
/// differently planned statement and attaching its statistics afterwards.
pub(in crate::engine::executor::select) fn with_analyzed_sources<R>(
    db: &Arc<Database>,
    txn: TxnView,
    original: &RrfFusion,
    plan: &PhysicalPlan,
    registry: &SessionRegistry,
    run: impl FnOnce() -> Result<R, QueryError>,
) -> Result<R, QueryError> {
    let _scoring = super::super::fulltext_context::ensure();
    let (_, weights) = bind_options(original)?;
    let mut spec = original.clone();
    let mut spools = Spools::new();
    for (i, branch) in spec.branches.iter_mut().enumerate() {
        crate::engine::executor::cte::with_cte_registry_extended(spools.clone(), || {
            prepare_source(
                db,
                txn,
                &mut branch.query,
                registry,
                &mut spools,
                weights[i] > 0.0,
            )
        })?;
    }
    let mut display = None;
    plan.walk(|node| {
        if let PhysicalPlan::RrfFusion { spec, .. } = node {
            display = Some(spec.clone());
        }
    });
    let display = display.ok_or_else(|| invalid("ANALYZE fusion graph is missing"))?;
    fn inputs(query: &QueryExpr) -> Vec<String> {
        struct Collect(Vec<String>);
        impl crate::parser::visit::AstVisitor for Collect {
            fn enter_select(&mut self, select: &Select) {
                self.0
                    .extend(select.from.iter().map(|table| table.name.clone()));
                self.0
                    .extend(select.joins.iter().map(|join| join.table.name.clone()));
            }
        }
        let mut collect = Collect(Vec::new());
        crate::parser::visit::visit_query(&mut collect, query);
        collect.0
    }
    let mut aliases = std::collections::HashMap::new();
    for (actual, expected) in spec.branches.iter().zip(&display.branches) {
        let actual = inputs(&actual.query);
        let expected = inputs(&expected.query);
        if actual.len() != expected.len() {
            return Err(invalid("ANALYZE input graph changed shape"));
        }
        for (actual, expected) in actual.into_iter().zip(expected) {
            if spools.contains_key(&actual) {
                if let Some(previous) = aliases.insert(actual, expected.clone()) {
                    if previous != expected {
                        return Err(invalid("ANALYZE spool binding is ambiguous"));
                    }
                }
            }
        }
    }
    for (actual, expected) in aliases {
        if let Some(result) = spools.get(&actual).cloned() {
            spools.insert(expected, result);
        }
    }
    crate::engine::executor::cte::with_cte_registry_extended(spools, run)
}
