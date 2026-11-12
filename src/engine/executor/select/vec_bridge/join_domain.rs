//! Necessary union domains for disjunctions over independent optional joins.
use super::*;
use crate::engine::executor::select::bloom::BloomFilter;
use crate::engine::expr::ValueKey;
use crate::planner::RuntimeFilter;

#[cfg(test)]
static UNION_DOMAIN_HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn integer(column: ColumnType) -> bool {
    matches!(
        column,
        ColumnType::Tiny
            | ColumnType::Short
            | ColumnType::MediumInt
            | ColumnType::Long
            | ColumnType::LongLong
    )
}

struct CteFilters<'a> {
    inner: Box<dyn BatchOperator + 'a>,
    template: Vec<PlanField>,
    filters: Vec<(RuntimeFilter, Vec<usize>)>,
    opened: bool,
}

pub(super) fn filterable_cte<'a>(
    inner: Box<dyn BatchOperator + 'a>,
    template: &[PlanField],
) -> Box<dyn BatchOperator + 'a> {
    Box::new(CteFilters {
        inner,
        template: template.to_vec(),
        filters: Vec::new(),
        opened: false,
    })
}

impl BatchOperator for CteFilters<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        self.inner.schema()
    }
    fn properties(&self) -> BatchProperties {
        self.inner.properties()
    }
    fn prefer_key_locality(&mut self, columns: &[usize]) -> bool {
        self.inner.prefer_key_locality(columns)
    }
    fn install_runtime_filter(&mut self, filter: RuntimeFilter) -> bool {
        if self.opened {
            return false;
        }
        let keys = match &filter {
            RuntimeFilter::BloomKey { keys, .. }
            | RuntimeFilter::ExactIntKey { keys, .. }
            | RuntimeFilter::KeySet { keys, .. } => keys,
        };
        // A NULL-accepting filter on a null-supplying join input needs a
        // separate outer-join proof. Decline it at this new source boundary.
        let null_safe = match &filter {
            RuntimeFilter::BloomKey { null_safe, .. }
            | RuntimeFilter::ExactIntKey { null_safe, .. }
            | RuntimeFilter::KeySet { null_safe, .. } => null_safe,
        };
        if null_safe.iter().any(|safe| *safe) {
            return false;
        }
        let Some(indices) = keys
            .iter()
            .map(|key| {
                let index = expr_column_index_in_template(key, &self.template)?;
                integer(self.template[index].column_type).then_some(index)
            })
            .collect::<Option<Vec<_>>>()
        else {
            return false;
        };
        if indices.is_empty() {
            return false;
        }
        self.filters.push((filter, indices));
        true
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.opened = true;
        loop {
            let Some(batch) = self.inner.next_batch()? else {
                self.filters.clear();
                return Ok(None);
            };
            if self.filters.is_empty() {
                return Ok(Some(batch));
            }
            let mut keys = Vec::with_capacity(8);
            let mut selected = Vec::with_capacity(batch.visible_rows());
            for row in batch.visible_indices() {
                let mut keep = true;
                for (filter, indices) in &self.filters {
                    keys.clear();
                    let null_safe = match filter {
                        RuntimeFilter::BloomKey { null_safe, .. }
                        | RuntimeFilter::ExactIntKey { null_safe, .. }
                        | RuntimeFilter::KeySet { null_safe, .. } => null_safe,
                    };
                    for (slot, &index) in indices.iter().enumerate() {
                        let column = &batch.columns()[index];
                        if column.is_null(row) && !null_safe.get(slot).copied().unwrap_or(false) {
                            keep = false;
                            break;
                        }
                        keys.push(ValueKey::from(&column.value_at(row)));
                    }
                    if !keep {
                        break;
                    }
                    keep = match filter {
                        RuntimeFilter::BloomKey { filter, .. } => filter.contains_value_keys(&keys),
                        RuntimeFilter::KeySet { filter, .. } => filter.contains(keys.as_slice()),
                        RuntimeFilter::ExactIntKey { filter, .. } => match keys.as_slice() {
                            [ValueKey::Int(value)] => filter.contains(*value),
                            _ => true,
                        },
                    };
                    if !keep {
                        break;
                    }
                }
                if keep {
                    selected.push(row as u32);
                }
            }
            analyze_stats::add_pending_runtime_filter_rejected(
                (batch.visible_rows() - selected.len()) as u64,
            );
            if selected.len() == batch.visible_rows() {
                return Ok(Some(batch));
            }
            if !selected.is_empty() {
                return Ok(Some(batch.with_selection(Selection::Indices(selected))));
            }
        }
    }
}

struct Branch<'a> {
    plan: &'a PhysicalPlan,
    keys: Vec<usize>,
    predicate: Arc<expr_batch::BatchExprProgram>,
}

/// Only warning-free scalar comparisons are evaluated in the domain pass.
/// Other functions, conversions, arithmetic and subqueries retain the ordinary
/// post-join evaluation point, including their errors and diagnostics.
fn safe_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => true,
        Expr::Literal(Literal::Int(_) | Literal::UInt(_) | Literal::Null | Literal::Bool(_)) => {
            true
        }
        Expr::BoundValue(value) => matches!(value, Value::Null | Value::Int(_) | Value::UInt(_)),
        Expr::Placeholder(index) => crate::engine::params::with_current_param(*index, |value| {
            matches!(value, Some(Value::Null | Value::Int(_) | Value::UInt(_)))
        }),
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => safe_predicate(expr),
        Expr::Binary { left, op, right } => {
            matches!(
                op,
                BinaryOp::And
                    | BinaryOp::Or
                    | BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
            ) && safe_predicate(left)
                && safe_predicate(right)
        }
        Expr::Function {
            name,
            args,
            distinct: false,
        } if name.eq_ignore_ascii_case("coalesce") => args.iter().all(safe_predicate),
        _ => false,
    }
}

fn split_or<'a>(expr: &'a Expr, terms: &mut Vec<&'a Expr>) {
    if let Expr::Binary {
        left,
        op: BinaryOp::Or,
        right,
    } = expr
    {
        split_or(left, terms);
        split_or(right, terms);
    } else {
        terms.push(expr);
    }
}

fn cte_rows(plan: &PhysicalPlan) -> Option<usize> {
    let PhysicalPlan::CteScan { name, .. } = peel_projection(plan) else {
        return None;
    };
    let cte = crate::engine::executor::cte::lookup_cte(name)?;
    Some(
        cte.spill
            .as_ref()
            .map_or_else(|| cte.nrows(), |spill| spill.row_count),
    )
}

fn substantial_build_work(bytes: f64, query_budget: usize) -> bool {
    bytes > query_budget.saturating_mul(2) as f64
}

pub(super) fn wrap_disjunction<'a>(
    input: &'a PhysicalPlan,
    predicate: &Expr,
    inner: Box<dyn BatchOperator + 'a>,
) -> Result<Box<dyn BatchOperator + 'a>, QueryError> {
    let mut terms = Vec::new();
    split_or(predicate, &mut terms);
    if !(2..=4).contains(&terms.len()) || terms.iter().any(|expr| !safe_predicate(expr)) {
        return Ok(inner);
    }
    let mut joins = Vec::new();
    let mut base = input;
    while let PhysicalPlan::HashJoin {
        left,
        right,
        join_type: crate::parser::JoinType::Left,
        build_side: BuildSide::Right,
        left_keys,
        right_keys,
        null_safe,
        residual: None,
        ..
    } = base
    {
        if left_keys.is_empty()
            || left_keys.len() != right_keys.len()
            || null_safe.iter().any(|safe| *safe)
        {
            return Ok(inner);
        }
        joins.push((right.as_ref(), left_keys.as_slice(), right_keys.as_slice()));
        base = left;
    }
    if joins.len() != terms.len() {
        return Ok(inner);
    }
    let Some(base_rows) = cte_rows(base) else {
        return Ok(inner);
    };
    let policy = crate::util::env::var("MYDB_DISJUNCTIVE_JOIN_DOMAIN").unwrap_or_default();
    if policy == "0" {
        return Ok(inner);
    }
    let base_template = plan_output_template(base);
    let left_keys = joins[0].1;
    let Some(base_indices) = left_keys
        .iter()
        .map(|key| {
            let index = expr_column_index_in_template(key, &base_template)?;
            integer(base_template[index].column_type).then_some(index)
        })
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(inner);
    };
    if base_indices.is_empty() {
        return Ok(inner);
    }
    let mut rows = 0usize;
    let mut build_bytes = 0.0;
    let mut branches = Vec::new();
    let mut used = vec![false; terms.len()];
    for (right, probe_keys, build_keys) in joins {
        if probe_keys != left_keys {
            return Ok(inner);
        }
        let Some(right_rows) = cte_rows(right) else {
            return Ok(inner);
        };
        rows = rows.saturating_add(right_rows);
        build_bytes += right_rows as f64 * (right.output_width() + 64.0);
        let template = plan_output_template(right);
        let Some(keys) = build_keys
            .iter()
            .map(|key| {
                let index = expr_column_index_in_template(key, &template)?;
                integer(template[index].column_type).then_some(index)
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(inner);
        };
        let schema = template_to_batch_schema(&template);
        let mut branch = None;
        for (index, term) in terms.iter().enumerate() {
            if used[index] {
                continue;
            }
            let outside_scope = crate::parser::visit::any_expr_matching(
                term,
                |expr| {
                    matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                        && expr_column_index_in_template(expr, &template).is_none_or(|index| {
                            !matches!(
                                template[index].column_type,
                                ColumnType::Tiny
                                    | ColumnType::Short
                                    | ColumnType::MediumInt
                                    | ColumnType::Long
                                    | ColumnType::LongLong
                                    | ColumnType::Decimal { .. }
                                    | ColumnType::Float
                                    | ColumnType::Double
                            )
                        })
                },
                crate::parser::visit::scalar_expr_children,
            );
            if outside_scope {
                continue;
            }
            // Compilation must bind every reference in this one RHS scope.
            let Some(program) = expr_batch::BatchExprProgram::compile(term, &schema)? else {
                continue;
            };
            let nulls = Batch::with_row_count(
                schema.clone(),
                (0..schema.len())
                    .map(|_| ColumnVector::from_values(vec![Value::Null]))
                    .collect(),
                1,
            );
            if program.eval_predicate_bitmap(&nulls)?.get(0) {
                return Ok(inner);
            }
            branch = Some((index, program));
            break;
        }
        let Some((index, program)) = branch else {
            return Ok(inner);
        };
        used[index] = true;
        branches.push(Branch {
            plan: right,
            keys,
            predicate: program,
        });
    }
    if base_rows < 65_536 || base_rows < rows || rows == 0 {
        return Ok(inner);
    }
    let bytes = BloomFilter::estimated_byte_size(rows);
    let Some(budget) = mem_budget::current_budget() else {
        return Ok(inner);
    };
    // Two extra replay passes need material spill work to amortize them.
    // "force" is a diagnostic cost override, never a type/NULL/budget override.
    if policy != "force" && !substantial_build_work(build_bytes, budget.limit()) {
        return Ok(inner);
    }
    if bytes > (budget.limit() / 4).min(128 * 1024 * 1024) {
        return Ok(inner);
    }
    Ok(Box::new(UnionDomain {
        inner,
        branches: Some(branches),
        keys: left_keys.to_vec(),
        rows,
        bytes,
        budget,
    }))
}

struct UnionDomain<'a> {
    inner: Box<dyn BatchOperator + 'a>,
    branches: Option<Vec<Branch<'a>>>,
    keys: Vec<Expr>,
    rows: usize,
    bytes: usize,
    budget: Arc<mem_budget::MemBudget>,
}

impl BatchOperator for UnionDomain<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        self.inner.schema()
    }
    fn properties(&self) -> BatchProperties {
        self.inner.properties()
    }
    fn install_runtime_filter(&mut self, filter: RuntimeFilter) -> bool {
        self.inner.install_runtime_filter(filter)
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if let Some(branches) = self.branches.take() {
            if let Ok(charge) =
                mem_budget::OwnedMemCharge::new_optional_pipeline(self.budget.clone(), self.bytes)
            {
                let mut bloom = BloomFilter::new(self.rows);
                bloom.attach_charge(Arc::new(charge));
                let mut keys = Vec::with_capacity(self.keys.len());
                for branch in branches {
                    let Some(mut input) = try_build_cte_batch_scan_unfiltered(branch.plan)? else {
                        return Err(crate::error::catalog::ER_INTERNAL_ERROR
                            .error("CTE domain source changed capability"));
                    };
                    while let Some(batch) = input.stream.next_batch()? {
                        crate::engine::cancel::check_cancelled()?;
                        let qualified = branch.predicate.eval_predicate_bitmap(&batch)?;
                        for row in batch.visible_indices() {
                            if !qualified.get(row) {
                                continue;
                            }
                            keys.clear();
                            for &index in &branch.keys {
                                let column = &batch.columns()[index];
                                if column.is_null(row) {
                                    break;
                                }
                                keys.push(ValueKey::from(&column.value_at(row)));
                            }
                            if keys.len() == branch.keys.len() {
                                bloom.insert(keys.as_slice());
                            }
                        }
                    }
                }
                self.inner.install_runtime_filter(RuntimeFilter::BloomKey {
                    keys: self.keys.clone(),
                    null_safe: vec![false; self.keys.len()],
                    filter: Arc::new(bloom),
                    rejected: None,
                });
                #[cfg(test)]
                UNION_DOMAIN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        self.inner.next_batch()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_replay_cost_requires_substantial_build_work() {
        let budget = 64 << 20;
        assert!(!substantial_build_work(budget as f64, budget));
        assert!(!substantial_build_work((budget * 2) as f64, budget));
        assert!(substantial_build_work((budget * 3) as f64, budget));
    }

    #[test]
    fn numeric_parameter_domains_have_the_same_eligibility_as_literals() {
        let crate::parser::Statement::Select(select) =
            crate::parser::parse_statement("SELECT 1 WHERE COALESCE(b.v,?)>? OR COALESCE(c.v,?)>?")
                .unwrap()
        else {
            panic!("SELECT")
        };
        crate::engine::params::with_auto_params(vec![Value::Int(0); 4], || {
            assert!(safe_predicate(select.selection.as_ref().unwrap()));
        });
    }

    #[test]
    fn spilled_union_domains_really_execute() {
        let _policy =
            crate::util::env::scoped_vars(&[("MYDB_DISJUNCTIVE_JOIN_DOMAIN", Some("force"))]);
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::open(dir.path()).unwrap());
        let columns = ["k", "lane", "v"]
            .into_iter()
            .map(|name| crate::types::Column {
                name: name.into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            })
            .collect::<Vec<_>>();
        let mut spools = std::collections::HashMap::new();
        crate::util::env::with_vars(&[("MYDB_CTE_SPILL_THRESHOLD_MB", Some("0"))], || {
            for (name, first, end) in [("a", 1, 70_000), ("b", 1, 12_000), ("c", 9_000, 22_000)] {
                let mut builder = crate::engine::executor::cte::CteSpillBuilder::new(
                    columns.clone(),
                    dir.path().to_path_buf(),
                );
                for key in first..=end {
                    builder
                        .push(vec![Value::Int(key), Value::Int(key % 3), Value::Int(1)])
                        .unwrap();
                }
                let result = builder.finish().unwrap();
                assert!(
                    result.spill.is_some(),
                    "{name} must read the on-disk source"
                );
                spools.insert(name.to_string(), Arc::new(result));
            }
        });
        let budget = Arc::new(mem_budget::MemBudget::new(64 << 20));
        mem_budget::with_query_budget(budget.clone(), || {
            crate::engine::executor::cte::with_cte_registry_extended(spools, || {
                let crate::parser::Statement::Select(select) = crate::parser::parse_statement(
                    "SELECT a.k,a.lane,a.v,b.v,c.v FROM a LEFT JOIN b ON a.k=b.k AND a.lane=b.lane LEFT JOIN c ON a.k=c.k AND a.lane=c.lane WHERE COALESCE(b.v,0)>0 OR COALESCE(c.v,0)>0 ORDER BY a.k"
                ).unwrap() else { panic!("SELECT") };
                let run = || {
                    let plan = crate::planner::plan_select(&db, &select).unwrap().physical;
                    execute_native_plan_values(
                        &db,
                        TxnView::read_only(
                            db.visible_scn(),
                            crate::storage::IsolationLevel::ReadCommitted,
                        ),
                        &plan,
                    )
                    .unwrap()
                    .unwrap()
                    .1
                };
                let expected = crate::util::env::with_vars(
                    &[("MYDB_DISJUNCTIVE_JOIN_DOMAIN", Some("0"))],
                    run,
                );
                let before = UNION_DOMAIN_HITS.load(std::sync::atomic::Ordering::Relaxed);
                let actual = run();
                assert_eq!(actual, expected);
                assert_eq!(actual.len(), 22_000);
                assert!(UNION_DOMAIN_HITS.load(std::sync::atomic::Ordering::Relaxed) > before);
                let crate::parser::Statement::Select(parameterized) = crate::parser::parse_statement(
                    "SELECT a.k,a.lane,a.v,b.v,c.v FROM a LEFT JOIN b ON a.k=b.k AND a.lane=b.lane LEFT JOIN c ON a.k=c.k AND a.lane=c.lane WHERE COALESCE(b.v,?)>? OR COALESCE(c.v,?)>? ORDER BY a.k"
                ).unwrap() else { panic!("SELECT") };
                crate::engine::params::with_auto_params(vec![Value::Int(0); 4], || {
                    let before = UNION_DOMAIN_HITS.load(std::sync::atomic::Ordering::Relaxed);
                    let plan = crate::planner::plan_select(&db, &parameterized)
                        .unwrap()
                        .physical;
                    let actual = execute_native_plan_values(
                        &db,
                        TxnView::read_only(
                            db.visible_scn(),
                            crate::storage::IsolationLevel::ReadCommitted,
                        ),
                        &plan,
                    )
                    .unwrap()
                    .unwrap()
                    .1;
                    assert_eq!(actual, expected);
                    assert!(
                        UNION_DOMAIN_HITS.load(std::sync::atomic::Ordering::Relaxed) > before,
                        "parameterized protocol constants must not silently disable the domain"
                    );
                });
            });
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn domain_cardinality_uses_spilled_rows() {
        let dir = tempfile::tempdir().unwrap();
        let columns = vec![crate::types::Column {
            name: "k".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }];
        let cte =
            crate::util::env::with_vars(&[("MYDB_CTE_SPILL_THRESHOLD_MB", Some("0"))], || {
                let mut builder = crate::engine::executor::cte::CteSpillBuilder::new(
                    columns.clone(),
                    dir.path().to_path_buf(),
                );
                builder.push(vec![Value::Int(7)]).unwrap();
                builder.finish().unwrap()
            });
        assert!(cte.spill.is_some());
        assert_eq!(
            cte.nrows(),
            0,
            "the resident API deliberately excludes the spilled file"
        );
        let plan = PhysicalPlan::CteScan {
            name: "domain_spilled".into(),
            alias: None,
            columns,
            filter: None,
            estimated_rows: 1.0,
            cost: crate::engine::cbo::Cost::scalar(1.0),
            projected_columns: None,
        };
        crate::engine::executor::cte::with_cte_registry_extended(
            std::collections::HashMap::from([("domain_spilled".into(), Arc::new(cte))]),
            || {
                assert_eq!(cte_rows(&plan), Some(1));
            },
        );
    }

    #[test]
    fn disjunctive_optional_domains_preserve_bags_nulls_and_negative_values() {
        let _policy =
            crate::util::env::scoped_vars(&[("MYDB_DISJUNCTIVE_JOIN_DOMAIN", Some("force"))]);
        use crate::engine::StorageQueryEngine;
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        for sql in [
            "CREATE TABLE domain_left (k BIGINT, lane INT, v INT)",
            "CREATE TABLE domain_right (k BIGINT, lane INT, v INT)",
            "CREATE TABLE domain_other (k BIGINT, lane INT, v INT)",
            "INSERT INTO domain_left SELECT n,MOD(n,3),n FROM generate_series(1,70000) g(n)",
            "INSERT INTO domain_left VALUES (1,1,123),(NULL,1,8),(2,NULL,9)",
            "INSERT INTO domain_right SELECT n,MOD(n,3),CASE WHEN MOD(n,4)=0 THEN NULL WHEN MOD(n,4)=1 THEN -1 ELSE 2 END FROM generate_series(1,12000) g(n)",
            "INSERT INTO domain_right VALUES (1,1,3),(1,1,-7),(2,2,4),(NULL,1,2)",
            "INSERT INTO domain_other SELECT n,MOD(n,3),CASE WHEN MOD(n,5)=0 THEN -1 ELSE 1 END FROM generate_series(9000,22000) g(n)",
            "INSERT INTO domain_other VALUES (1,1,-9),(2,2,8),(NULL,1,3)",
        ] { engine.execute(sql).unwrap(); }
        let query = "WITH a AS (SELECT * FROM domain_left), b AS (SELECT * FROM domain_right), c AS (SELECT * FROM domain_other) SELECT a.k,a.lane,a.v,b.v,c.v FROM a LEFT JOIN b ON a.k=b.k AND a.lane=b.lane LEFT JOIN c ON a.k=c.k AND a.lane=c.lane WHERE COALESCE(b.v,0)>0 OR COALESCE(c.v,0)>0 ORDER BY a.k,a.lane,a.v,b.v,c.v";
        let rows = || engine.execute(query).unwrap().materialize().unwrap();
        crate::util::env::with_vars(&[("MYDB_CTE_INLINE", Some("0"))], || {
            let expected =
                crate::util::env::with_vars(&[("MYDB_DISJUNCTIVE_JOIN_DOMAIN", Some("0"))], rows);
            let crate::engine::api::QueryResponse::ResultSet(expected) = expected else {
                panic!("rows")
            };
            for spill in ["0", "64"] {
                crate::util::env::with_vars(
                    &[("MYDB_CTE_SPILL_THRESHOLD_MB", Some(spill))],
                    || {
                        let before = UNION_DOMAIN_HITS.load(std::sync::atomic::Ordering::Relaxed);
                        let crate::engine::api::QueryResponse::ResultSet(actual) = rows() else {
                            panic!("rows")
                        };
                        assert_eq!(actual.rows, expected.rows);
                        assert!(
                            UNION_DOMAIN_HITS.load(std::sync::atomic::Ordering::Relaxed) > before,
                            "the union domain must actually execute for both CTE storage modes"
                        );
                    },
                );
            }
            for query in [
                query.replace("COALESCE(b.v,0)>0", "COALESCE(b.v,0)>=0"),
                query.replace("COALESCE(b.v,0)>0", "a.v>0"),
                query.replace("a.k=c.k AND a.lane=c.lane", "a.lane=c.k AND a.k=c.lane"),
                query.replace("a.k=b.k", "a.k<=>b.k"),
            ] {
                let run = || engine.execute(&query).unwrap().materialize().unwrap();
                let expected = crate::util::env::with_vars(
                    &[("MYDB_DISJUNCTIVE_JOIN_DOMAIN", Some("0"))],
                    run,
                );
                let before = UNION_DOMAIN_HITS.load(std::sync::atomic::Ordering::Relaxed);
                let actual = run();
                let (
                    crate::engine::api::QueryResponse::ResultSet(expected),
                    crate::engine::api::QueryResponse::ResultSet(actual),
                ) = (expected, actual)
                else {
                    panic!("rows")
                };
                assert_eq!(actual.rows, expected.rows, "{query}");
                assert_eq!(UNION_DOMAIN_HITS.load(std::sync::atomic::Ordering::Relaxed), before,
                    "a NULL-accepting disjunct or incompatible key tuple must keep the original path");
            }
        });
    }
}
