//! Fuse a single aggregate consumer with a native set-operation producer.
//! No partial AVG rewrite: the canonical accumulator sees the original rows.
use super::*;
use crate::planner::PhysicalPlan;

#[cfg(test)]
pub(crate) static HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn scalar_argument(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_)
        | Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Placeholder(_)
        | Expr::Wildcard => true,
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            scalar_argument(expr)
        }
        Expr::Binary { left, right, .. } => scalar_argument(left) && scalar_argument(right),
        Expr::Function { args, .. } => args.iter().all(scalar_argument),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_deref().is_none_or(scalar_argument)
                && conditions.iter().chain(results).all(scalar_argument)
                && else_result.as_deref().is_none_or(scalar_argument)
        }
        _ => false,
    }
}

fn prepared_source<'q>(statement: impl Into<crate::parser::QueryRef<'q>>) -> bool {
    let statement = statement.into();
    match statement {
        crate::parser::QueryRef::Select(select) => {
            select.derived.is_none()
                && select.rls_security_context.is_none()
                && select.rls_expression_settings.is_none()
                && select.joins.iter().all(|join| join.derived.is_none())
                && !select.for_update
                && !select.sql_calc_found_rows
                && select.lock_clause.is_none()
        }
        crate::parser::QueryRef::SetOperation(set) => {
            prepared_source(&set.left) && prepared_source(&set.right)
        }
        _ => false,
    }
}

pub(super) fn try_materialize<'q>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: impl Into<crate::parser::QueryRef<'q>>,
    cte: &CteDef,
) -> Result<Option<CteResult>, QueryError> {
    let statement = statement.into();
    let crate::parser::QueryRef::Select(select) = statement else {
        return Ok(None);
    };
    let txn = cte_statement_view(db, session);
    with_cte_materialization(|| try_execute(db, txn, select))?
        .map(|response| route_response_to_cte_result(db, response, cte))
        .transpose()
}

/// An aggregate can consume a derived set expression directly in both
/// CTE materialization and ordinary SELECT execution. Capability is checked
/// before opening either branch; the same accumulator retains AVG semantics.
pub(crate) fn try_execute(
    db: &Arc<Database>,
    txn: crate::storage::TxnView,
    select: &Select,
) -> Result<Option<QueryResponse>, QueryError> {
    let Some(crate::parser::SubQuery::SetOp(source)) = &select.derived else {
        return Ok(None);
    };
    if select.from.len() != 1
        || select
            .from
            .iter()
            .any(|table| !table.column_aliases.is_empty())
        || !select.joins.is_empty()
        || !select.grouping_sets.is_empty()
        || select.group_by_modifier != crate::parser::GroupByModifier::None
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.distinct
        || select.for_update
        || select.lock_clause.is_some()
        || select.sql_calc_found_rows
        || !select.query_hints.is_empty()
        || select_has_nondeterministic(select)
        || !prepared_source(source)
        || !statement_is_deterministic_for_cte_cache(source)
    {
        return Ok(None);
    }
    // The derived alias belongs to this query boundary, while the native
    // producer exposes the set expression's output names. Bind only that
    // one scope; nested queries retain their own ordinary materializer.
    let alias = select.from[0]
        .alias
        .as_deref()
        .unwrap_or(&select.from[0].name);
    let mut bound = select.clone();
    let bind = |expr: &mut Expr| {
        crate::parser::visit::try_rewrite_value_expr(expr, |node| {
            while let Expr::GroupKeyRef { expr, .. } = node {
                *node = *expr.clone();
            }
            match node {
                Expr::CompoundIdentifier(parts) => {
                    if parts.len() != 2 || !parts[0].eq_ignore_ascii_case(alias) {
                        return Err(());
                    }
                    *node = Expr::Identifier(parts[1].clone());
                }
                Expr::Subquery(_)
                | Expr::InSubquery { .. }
                | Expr::Exists { .. }
                | Expr::QuantifiedComparison { .. } => return Err(()),
                _ => {}
            }
            Ok(true)
        })
    };
    for item in &mut bound.projection {
        if let SelectItem::Expr { expr, .. } = item {
            if bind(expr).is_err() {
                return Ok(None);
            }
        }
    }
    for key in &mut bound.group_by {
        if bind(key).is_err() {
            return Ok(None);
        }
    }
    if let Some(predicate) = &mut bound.selection {
        if bind(predicate).is_err() {
            return Ok(None);
        }
    }
    let select = &bound;
    if select
        .group_by
        .iter()
        .any(|key| !matches!(key, Expr::Identifier(_)))
    {
        return Ok(None);
    }
    if select.projection.is_empty() || !select.projection.iter().all(|item| {
        matches!(item, SelectItem::Expr { expr, .. }
            if select.group_by.iter().any(|key| crate::engine::expr_program::expr_semantically_eq(key, expr)))
        || matches!(item, SelectItem::Expr { expr: Expr::Function { name, args, .. }, .. }
            if matches!(name.to_ascii_uppercase().as_str(), "SUM" | "AVG" | "COUNT"
                | "STDDEV_POP" | "STDDEV_SAMP" | "VAR_POP" | "VAR_SAMP" | "BIT_AND" | "BIT_OR" | "BIT_XOR")
                && args.iter().all(scalar_argument))
    }) { return Ok(None); }
    let Some(mut input) = crate::planner::plan_query_physical(db, source)? else {
        return Ok(None);
    };
    // Retain the consumer's exact WHERE evaluation after set semantics and
    // before grouping, including predicates also pushed into the branches.
    if let Some(predicate) = &select.selection {
        input = PhysicalPlan::Filter {
            estimated_rows: crate::planner::estimated_rows(&input),
            cost: crate::planner::plan_cost(&input),
            input: Box::new(input),
            predicate: predicate.clone(),
        };
    }
    let input_cost = crate::planner::plan_cost(&input);
    let groups = if select.group_by.is_empty() {
        1.0
    } else {
        crate::planner::estimate_group_by_rows_for_plan(
            &input,
            &select.group_by,
            crate::planner::estimated_rows(&input),
        )
    };
    let cost = crate::engine::cbo::operator_cost::hash_aggregate(
        &crate::engine::cbo::CboContext::from_db(db),
        &input_cost,
        crate::planner::estimated_rows(&input),
        groups,
        select.projection.len() as f64 * 32.0,
    );
    let mut plan = PhysicalPlan::Projection {
        input: Box::new(PhysicalPlan::HashAggregate {
            input: Box::new(input),
            group_by: select.group_by.clone(),
            having: None,
            aggregates: crate::planner::collect_aggregate_calls(select),
            estimated_rows: groups,
            cost: cost.clone(),
        }),
        projection: select.projection.clone(),
        distinct: false,
        estimated_rows: groups,
        cost,
    };
    crate::planner::parallelize_fused_aggregate(&mut plan);
    if !super::super::select::vec_bridge::preflight_query_output_supported(&plan)? {
        return Ok(None);
    }
    let budget = super::super::mem_budget::current_budget()
        .unwrap_or_else(|| Arc::new(super::super::mem_budget::MemBudget::from_env()));
    let stream = {
        let context =
            super::super::select::execution_context::ExecutionContext::capture_with_budget(
                txn, budget,
            );
        super::super::select::vec_bridge::try_stream_native_plan(
            Arc::clone(db),
            txn,
            Arc::new(plan),
            context,
        )
    }?;
    let Some(stream) = stream else {
        return Ok(None);
    };
    #[cfg(test)]
    HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(Some(QueryResponse::StreamingResultSet(stream)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        executor::vec::config::{with_vec_mode, VecMode},
        StorageQueryEngine,
    };
    #[test]
    fn grouped_set_consumer_fuses_without_changing_distinct_null_or_decimal_semantics() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        for sql in ["CREATE TABLE fused_groups(g INT,v DECIMAL(18,4),n INT)",
            "INSERT INTO fused_groups VALUES(1,1.2500,2),(1,1.2500,2),(1,2.5000,4),(2,NULL,2),(NULL,3.0000,4)"] {engine.execute(sql).unwrap();}
        for source in [
            "SELECT g,v,n FROM fused_groups UNION ALL SELECT g,v,n FROM fused_groups WHERE n=2",
            "SELECT g,v,n FROM fused_groups UNION SELECT g,v,n FROM fused_groups WHERE n=2",
            "SELECT g,v,n FROM fused_groups INTERSECT SELECT g,v,n FROM fused_groups WHERE n=2",
            "SELECT g,v,n FROM fused_groups EXCEPT SELECT g,v,n FROM fused_groups WHERE n=2",
            "SELECT g,v,n FROM fused_groups WHERE n<0 UNION SELECT g,v,n FROM fused_groups WHERE n<0",
        ] {
            let sql=format!("SELECT g,SUM(v) sv,AVG(v*n) av,COUNT(*) ct,COUNT(v) cv FROM ({source}) x WHERE g IN (1,2) OR g IS NULL GROUP BY g");
            let run=|mode,query:&str|with_vec_mode(mode,||{
                let QueryResponse::ResultSet(mut r)=engine.execute(query).unwrap().materialize().unwrap()else{panic!()};
                r.rows.sort_by_key(|r|format!("{r:?}"));r
            });
            let before=HITS.load(std::sync::atomic::Ordering::Relaxed);
            let actual=run(VecMode::On,&sql);
            assert!(HITS.load(std::sync::atomic::Ordering::Relaxed)>before,"{source}");
            let expected=run(VecMode::Off,&format!("{sql} LIMIT 1000000"));
            assert_eq!(actual.columns,expected.columns,"{source}");assert_eq!(actual.rows,expected.rows,"{source}");
            let cte=format!("WITH a AS ({sql}) SELECT a.g,a.sv,a.av,a.ct,a.cv FROM a JOIN a b ON a.g<=>b.g");
            let before=HITS.load(std::sync::atomic::Ordering::Relaxed);
            let actual=run(VecMode::On,&cte);
            assert!(HITS.load(std::sync::atomic::Ordering::Relaxed)>before,"shared filtered grouped CTE: {source}");
            let expected=run(VecMode::Off,&cte);
            assert_eq!(actual.columns,expected.columns);assert_eq!(actual.rows,expected.rows);
        }
        let Statement::Select(invalid)=crate::parser::parse_statement("SELECT g,n,SUM(v) FROM (SELECT g,v,n FROM fused_groups UNION SELECT g,v,n FROM fused_groups) x GROUP BY g").unwrap()else{panic!()};
        let db = engine.db_for_tests();
        assert!(
            try_execute(
                &db,
                TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted),
                &invalid
            )
            .unwrap()
            .is_none(),
            "ungrouped projections retain normal SQL validation"
        );
    }
    #[test]
    fn grouped_joined_union_consumer_uses_the_native_source() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        for sql in [
            "CREATE TABLE fusion_dim(k INT,period INT,brand INT,cls INT,category INT,maker INT,label VARCHAR(12))",
            "CREATE TABLE fusion_lines(invoice INT,k INT,qty INT,total DECIMAL(18,2))",
            "CREATE TABLE fusion_returns(invoice INT,k INT,qty INT,total DECIMAL(18,2))",
            "INSERT INTO fusion_dim VALUES(1,1,3,4,5,6,'keep'),(2,2,3,4,5,6,'keep')",
            "INSERT INTO fusion_lines VALUES(1,1,3,10.25),(2,2,4,15.50),(3,2,NULL,NULL)",
            "INSERT INTO fusion_returns VALUES(1,1,1,2.25)",
        ] { engine.execute(sql).unwrap(); }
        let branch="SELECT d.period,d.brand,d.cls,d.category,d.maker,l.qty-COALESCE(r.qty,0) qty,l.total-COALESCE(r.total,0.0) amount FROM fusion_lines l JOIN fusion_dim d ON l.k=d.k LEFT JOIN fusion_returns r ON l.invoice=r.invoice AND l.k=r.k WHERE d.label='keep'";
        let query=format!("WITH grouped AS (SELECT period,brand,cls,category,maker,SUM(qty) qty,SUM(amount) amount FROM ({branch} UNION {branch} UNION {branch}) detail GROUP BY period,brand,cls,category,maker) SELECT a.brand,a.qty,b.qty,a.amount,b.amount FROM grouped a JOIN grouped b ON a.brand=b.brand AND a.cls=b.cls AND a.category=b.category AND a.maker=b.maker WHERE a.period=2 AND b.period=1");
        let before = HITS.load(std::sync::atomic::Ordering::Relaxed);
        let actual = with_vec_mode(VecMode::On, || {
            engine.execute(&query).unwrap().materialize().unwrap()
        });
        assert!(
            HITS.load(std::sync::atomic::Ordering::Relaxed) > before,
            "{query}"
        );
        let expected = with_vec_mode(VecMode::Off, || {
            engine.execute(&query).unwrap().materialize().unwrap()
        });
        let QueryResponse::ResultSet(actual) = actual else {
            panic!()
        };
        let QueryResponse::ResultSet(expected) = expected else {
            panic!()
        };
        assert_eq!(actual.columns, expected.columns);
        assert_eq!(actual.rows, expected.rows);
    }
}
