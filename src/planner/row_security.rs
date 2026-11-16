//! Relational authorization programs retain the security boundary while
//! sharing the ordinary hash/semi join kernels and their memory accounting.
use super::*;

pub(super) fn lower_authorization_joins(
    db: &Database,
    plan: &mut PhysicalPlan,
) -> Result<bool, QueryError> {
    let mut changed = false;
    let authorization_exchange = matches!(plan, PhysicalPlan::Exchange { input, .. }
        if matches!(input.as_ref(), PhysicalPlan::Apply { predicate: Expr::Function { name, .. }, .. }
            if name == "__MYDB_RLS_GUARD"));
    for child in plan_children_mut(plan) {
        changed |= lower_authorization_joins(db, child)?;
    }
    if authorization_exchange {
        // The set probe belongs above the parallel scan exchange, matching
        // the ordinary semi-join topology. Do not rebuild the authorization
        // hash table inside worker/batch boundaries.
        if let PhysicalPlan::Exchange { input, .. } = plan {
            if matches!(input.as_ref(), PhysicalPlan::HashJoin { .. }) {
                let placeholder = PhysicalPlan::ValuesScan {
                    rows: vec![],
                    columns: vec![],
                    filter: None,
                    estimated_rows: 0.0,
                    cost: crate::engine::cbo::Cost::ZERO,
                };
                let mut join = std::mem::replace(input.as_mut(), placeholder);
                if let PhysicalPlan::HashJoin { left, .. } = &mut join {
                    std::mem::swap(input, left);
                    **left = plan.clone();
                }
                *plan = join;
                return Ok(true);
            }
        }
    }
    let PhysicalPlan::Apply {
        outer,
        predicate,
        estimated_rows,
        cost,
    } = plan
    else {
        return Ok(changed);
    };
    let Some(replacement) = authorization_join(db, outer, predicate, *estimated_rows, *cost)?
    else {
        return Ok(changed);
    };
    *plan = replacement;
    Ok(true)
}

fn authorization_join(
    db: &Database,
    outer: &PhysicalPlan,
    predicate: &Expr,
    estimated_rows: f64,
    cost: crate::engine::cbo::Cost,
) -> Result<Option<PhysicalPlan>, QueryError> {
    let Expr::Function {
        name,
        args,
        distinct: false,
    } = predicate
    else {
        return Ok(None);
    };
    if name != "__MYDB_RLS_GUARD" || args.len() != 2 {
        return Ok(None);
    }
    let Expr::Literal(crate::parser::Literal::UInt(policy_table)) = &args[0] else {
        return Ok(None);
    };
    let Expr::Exists { subquery, negated } = &args[1] else {
        return Ok(None);
    };
    // This proof permits no executable expression to escape its guard. More
    // complex predicates retain Apply, including functions, LIMIT, aggregates,
    // locking reads, historical inputs and derived query blocks.
    if subquery.from.len() != 1
        || !subquery.joins.is_empty()
        || subquery.derived.is_some()
        || subquery.values.is_some()
        || subquery.json_table.is_some()
        || !subquery.group_by.is_empty()
        || !subquery.grouping_sets.is_empty()
        || subquery.having.is_some()
        || !subquery.order_by.is_empty()
        || subquery.limit.is_some()
        || subquery.offset.is_some()
        || subquery.for_update
        || subquery.lock_clause.is_some()
        || !subquery.named_windows.is_empty()
        || subquery.sql_calc_found_rows
        || subquery.projection.iter().any(|item| {
            !matches!(
                item,
                SelectItem::Expr {
                    expr: Expr::Literal(_),
                    ..
                }
            )
        })
    {
        return Ok(None);
    }
    let source = &subquery.from[0];
    if source.sample.is_some() || source.system_time.is_some() || source.generate_series.is_some() {
        return Ok(None);
    }
    let tables = collect_physical_plan_tables(outer);
    if tables.len() != 1 || tables[0].table_id != *policy_table {
        return Ok(None);
    }
    let outer_table = &tables[0];
    let inner_schema = db.table(&source.name)?;
    // Equal fixed numeric types have identical hash/equality semantics and
    // cannot produce conversion errors for an unauthorized target row.
    let inner_alias = source.alias.as_deref().unwrap_or(&inner_schema.name);
    let outer_alias = outer_table
        .alias
        .as_deref()
        .unwrap_or(&outer_table.schema.name);
    if inner_alias.eq_ignore_ascii_case(outer_alias) {
        return Ok(None);
    }
    let Some(Expr::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    }) = &subquery.selection
    else {
        return Ok(None);
    };
    let column =
        |expr: &Expr, alias: &str, schema: &TableSchema, allow_unqualified: bool| match expr {
            Expr::CompoundIdentifier(parts)
                if parts.len() == 2 && parts[0].eq_ignore_ascii_case(alias) =>
            {
                schema.column_index(&parts[1]).ok()
            }
            Expr::Identifier(name) if allow_unqualified => schema.column_index(name).ok(),
            _ => None,
        };
    let pair = |left: &Expr, right: &Expr| {
        Some((
            column(left, outer_alias, &outer_table.schema, false)?,
            column(right, inner_alias, &inner_schema, true)?,
            left.clone(),
            right.clone(),
        ))
    };
    let Some((outer_position, inner_position, outer_key, inner_key)) =
        pair(left, right).or_else(|| pair(right, left))
    else {
        return Ok(None);
    };
    let outer_column = &outer_table.schema.columns[outer_position];
    let inner_column = &inner_schema.columns[inner_position];
    if outer_column.data_type != inner_column.data_type
        || outer_column.unsigned != inner_column.unsigned
        || !matches!(
            outer_column.data_type,
            DataTypeKind::TinyInt
                | DataTypeKind::SmallInt
                | DataTypeKind::MediumInt
                | DataTypeKind::Int
                | DataTypeKind::BigInt
                | DataTypeKind::Bool
        )
    {
        return Ok(None);
    }
    let _guard = crate::engine::rls::PolicyScope::enter(*policy_table)?;
    let mut inner = subquery.as_ref().clone();
    inner.selection = None;
    inner.projection = vec![SelectItem::Expr {
        expr: inner_key.clone(),
        alias: None,
    }];
    let right = match plan_select(db, &inner)?.physical {
        // Projecting a single unchanged key is only an output-shape wrapper;
        // retain the underlying scan so existing batch/semi-join kernels can
        // consume it directly instead of materializing PlanRows.
        PhysicalPlan::Projection { input, .. } => *input,
        plan => plan,
    };
    Ok(Some(PhysicalPlan::HashJoin {
        left: Box::new(outer.clone()),
        right: Box::new(right),
        join_type: if *negated {
            JoinType::Anti
        } else {
            JoinType::Semi
        },
        left_keys: vec![outer_key],
        right_keys: vec![inner_key],
        null_safe: vec![false],
        build_side: BuildSide::Right,
        residual: None,
        output_projection: None,
        estimated_rows,
        cost,
    }))
}
