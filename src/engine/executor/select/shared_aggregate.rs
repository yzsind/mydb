//! Statement-local common grouped subexpressions. Keep one spill-capable CTE
//! spool; each reference retains its own alias, filter and scan. Nothing is
//! cached across executions, parameter bindings or transaction snapshots.
use super::*;
use crate::parser::{CteDef, GroupByModifier, QueryExpr};
use crate::storage::schema::{ColumnSchema, DataTypeKind, TableSchema};
use std::collections::HashSet;

pub(super) struct SharedAggregates {
    pub(super) definitions: Vec<(CteDef, usize)>,
    pub(super) body: Select,
    pub(super) outer_probe: Option<Select>,
    pub(super) column_overrides: Vec<Vec<Column>>,
}

fn safe_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(name) => !name.starts_with('@'),
        Expr::CompoundIdentifier(parts) => parts.iter().all(|part| !part.starts_with('@')),
        Expr::Literal(_) | Expr::BoundValue(_) | Expr::Placeholder(_) | Expr::Wildcard => true,
        Expr::Binary { left, right, .. } => safe_expr(left) && safe_expr(right),
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => safe_expr(expr),
        Expr::Between {
            expr, low, high, ..
        } => safe_expr(expr) && safe_expr(low) && safe_expr(high),
        Expr::Function { name, args, .. } => {
            matches!(
                name.to_ascii_uppercase().as_str(),
                "SUM" | "AVG" | "COUNT" | "MIN" | "MAX"
            ) && args.iter().all(safe_expr)
        }
        _ => false,
    }
}

fn safe_tree(select: &Select) -> bool {
    !select.for_update
        && select.lock_clause.is_none()
        && !select.sql_calc_found_rows
        && !select.no_merge
        && select.query_hints.is_empty()
        && select.hint_warnings.is_empty()
        && select.values.is_none()
        && select.json_table.is_none()
        && select.named_windows.is_empty()
        && select
            .from
            .iter()
            .chain(select.joins.iter().map(|join| &join.table))
            .all(|table| {
                table.system_time.is_none()
                    && table.generate_series.is_none()
                    && table.index_hints.is_empty()
            })
        && select.projection.iter().all(|item| match item {
            SelectItem::Expr { expr, .. } => safe_expr(expr),
            _ => true,
        })
        && select
            .selection
            .iter()
            .chain(select.having.iter())
            .chain(&select.group_by)
            .chain(select.order_by.iter().map(|item| &item.expr))
            .all(safe_expr)
        && select
            .derived
            .as_ref()
            .is_none_or(|query| matches!(query, QueryExpr::Select(inner) if safe_tree(inner)))
        && select.joins.iter().all(|join| {
            !join.lateral
                && join.condition.as_ref().is_none_or(safe_expr)
                && join.derived.as_ref().is_none_or(
                    |query| matches!(query, QueryExpr::Select(inner) if safe_tree(inner)),
                )
        })
}

fn constant(expr: &Expr) -> Option<Value> {
    if let Some(value) = crate::engine::expr::expr_literal_value(expr) {
        return Some(value);
    }
    let Expr::Binary { left, op, right } = expr else {
        return None;
    };
    let (Value::Int(a), Value::Int(b)) = (constant(left)?, constant(right)?) else {
        return None;
    };
    Some(Value::Int(match op {
        BinaryOp::Add => a.checked_add(b)?,
        BinaryOp::Sub => a.checked_sub(b)?,
        BinaryOp::Mul => a.checked_mul(b)?,
        _ => return None,
    }))
}

fn exact(column: &ColumnSchema) -> bool {
    matches!(
        column.data_type,
        DataTypeKind::TinyInt
            | DataTypeKind::SmallInt
            | DataTypeKind::MediumInt
            | DataTypeKind::Int
            | DataTypeKind::BigInt
            | DataTypeKind::Bool
            | DataTypeKind::Decimal {
                precision: Some(1..=38),
                ..
            }
    )
}

type Tables<'a> = Vec<(&'a crate::parser::TableRef, Arc<TableSchema>)>;
fn column<'a>(expr: &Expr, tables: &'a Tables<'_>) -> Option<((usize, usize), &'a ColumnSchema)> {
    if let Expr::GroupKeyRef { expr, .. } = expr {
        return column(expr, tables);
    }
    let (qualifier, name) = match expr {
        Expr::Identifier(name) => (None, name.as_str()),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => (
            Some(parts[parts.len() - 2].as_str()),
            parts.last()?.as_str(),
        ),
        _ => return None,
    };
    let mut found = None;
    for (table_index, (table, schema)) in tables.iter().enumerate() {
        if qualifier.is_some_and(|q| {
            !q.eq_ignore_ascii_case(
                table
                    .alias
                    .as_deref()
                    .unwrap_or_else(|| table.name.rsplit('.').next().unwrap_or(&table.name)),
            )
        }) {
            continue;
        }
        if let Some((index, col)) = schema
            .columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.name.eq_ignore_ascii_case(name))
        {
            if found.is_some() {
                return None;
            }
            found = Some(((table_index, index), col));
        }
    }
    found
}

fn numeric(expr: &Expr, tables: &Tables<'_>) -> bool {
    column(expr, tables).is_some_and(|(_, col)| exact(col))
        || constant(expr).is_some_and(|value| match value {
            Value::Int(_)
            | Value::UInt(_)
            | Value::DecimalCompact(..)
            | Value::Decimal128(..)
            | Value::Null => true,
            Value::Decimal(_) => value.exact_decimal().is_some(),
            _ => false,
        })
}

fn predicate(expr: &Expr, tables: &Tables<'_>) -> bool {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And | BinaryOp::Or,
            right,
        } => predicate(left, tables) && predicate(right, tables),
        Expr::Binary {
            left,
            op:
                BinaryOp::Eq
                | BinaryOp::NullSafeEq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq,
            right,
        } => numeric(left, tables) && numeric(right, tables),
        Expr::Between {
            expr, low, high, ..
        } => numeric(expr, tables) && numeric(low, tables) && numeric(high, tables),
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => column(expr, tables).is_some(),
        _ => false,
    }
}

/// Admit warning-free, exact grouped leaves only. In particular SUM(text),
/// casts, volatile/UDF calls, windows, LIMIT, correlations and locking reads
/// cannot silently lose evaluations or diagnostics through sharing.
fn identity(db: &Database, select: &Select) -> Option<Select> {
    if select.group_by.is_empty()
        || !select.grouping_sets.is_empty()
        || select.group_by_modifier != GroupByModifier::None
        || select.derived.is_some()
        || select.having.is_some()
        || select.limit.is_some()
        || select.offset.is_some()
        || !select.order_by.is_empty()
        || select.distinct
        || select.joins.iter().any(|join| join.derived.is_some())
    {
        return None;
    }
    let tables = select
        .from
        .iter()
        .chain(select.joins.iter().map(|join| &join.table))
        .map(|table| {
            if super::super::cte::has_cte(&table.name) {
                return None;
            }
            let schema = db.table(&table.name).ok()?;
            (!schema.system
                && schema.table_type.eq_ignore_ascii_case("BASE TABLE")
                && schema
                    .columns
                    .iter()
                    .all(|column| column.generated_expr.is_none()))
            .then_some((table, schema))
        })
        .collect::<Option<Tables<'_>>>()?;
    if tables.is_empty() {
        return None;
    }
    let groups = select
        .group_by
        .iter()
        .map(|expr| column(expr, &tables).map(|(key, _)| key))
        .collect::<Option<HashSet<_>>>()?;
    let mut aggregate = false;
    for item in &select.projection {
        let SelectItem::Expr { expr, .. } = item else {
            return None;
        };
        if let Some((key, _)) = column(expr, &tables) {
            if !groups.contains(&key) {
                return None;
            }
        } else if let Expr::Function {
            name,
            args,
            distinct: false,
        } = expr
        {
            let name = name.to_ascii_uppercase();
            if !matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") || args.len() != 1
            {
                return None;
            }
            if name == "COUNT" && matches!(args[0], Expr::Wildcard) {
                aggregate = true;
                continue;
            }
            let (_, col) = column(&args[0], &tables)?;
            if name != "COUNT" && !exact(col) {
                return None;
            }
            aggregate = true;
        } else {
            return None;
        }
    }
    if !aggregate
        || !select
            .selection
            .iter()
            .chain(
                select
                    .joins
                    .iter()
                    .filter_map(|join| join.condition.as_ref()),
            )
            .all(|expr| predicate(expr, &tables))
    {
        return None;
    }
    let mut normalized = select.clone();
    // Resolve parameter *values*, not their occurrence positions. Keep SQL
    // value types in the key; different bindings must never share a spool.
    for expr in normalized.selection.iter_mut().chain(
        normalized
            .joins
            .iter_mut()
            .filter_map(|join| join.condition.as_mut()),
    ) {
        crate::parser::visit::rewrite_value_expr(expr, |expr| {
            if let Some(value) = constant(expr) {
                *expr = Expr::BoundValue(value);
                false
            } else {
                true
            }
        });
    }
    Some(normalized)
}

// Fuse warning-free scalar aggregates over the same unindexed source. Each
// conditional aggregate retains its own filter and NULL semantics; consumers
// still see scalar subqueries with the original result column descriptors.
fn factor_scalar(db: &Database, select: &Select) -> Option<SharedAggregates> {
    if select.from.len() > 1
        || select.distinct
        || select.limit.is_some()
        || select.offset.is_some()
        || !select.order_by.is_empty()
        || planner::select_has_aggregate(db, select)
        || select.for_update
        || select.lock_clause.is_some()
        || select.sql_calc_found_rows
        || select.derived.is_some()
        || !select.joins.is_empty()
        || !select.group_by.is_empty()
        || !select.grouping_sets.is_empty()
        || select.having.is_some()
        || select.values.is_some()
        || select.json_table.is_some()
        || !select.named_windows.is_empty()
        || !select.query_hints.is_empty()
        || select.projection.iter().all(|item| {
            !matches!(item, SelectItem::Expr { expr, .. }
            if crate::parser::visit::value_expr_contains_subquery(expr))
        })
        || crate::engine::params::streaming_param_source_active()
        || !super::super::cte::current_cte_registry().is_empty()
        || !crate::util::env::flag("MYDB_SHARED_SCALAR_AGGREGATE", true)
    {
        return None;
    }

    // Probe at most one outer row before eagerly sharing independent
    // aggregates. Empty outer inputs must never trigger a large fact scan.
    let outer_probe = if let Some(table) = select.from.first() {
        let schema = db.table(&table.name).ok()?;
        if schema.system
            || schema.row_security.enabled
            || !schema.table_type.eq_ignore_ascii_case("BASE TABLE")
            || schema
                .columns
                .iter()
                .any(|column| column.generated_expr.is_some())
            || table.system_time.is_some()
            || table.sample.is_some()
            || table.generate_series.is_some()
            || !table.column_aliases.is_empty()
        {
            return None;
        }
        let tables = vec![(table, schema)];
        if select
            .selection
            .as_ref()
            .is_some_and(|filter| !predicate(filter, &tables))
        {
            return None;
        }
        let mut probe = select.clone();
        probe.projection = vec![SelectItem::Expr {
            expr: Expr::Literal(Literal::Int(1)),
            alias: None,
        }];
        probe.limit = Some(crate::parser::LimitValue::Rows(1));
        Some(probe)
    } else {
        if select.selection.is_some() {
            return None;
        }
        None
    };

    fn candidate(db: &Database, query: &Select) -> Option<(Select, Expr)> {
        if !safe_tree(query)
            || query.from.len() != 1
            || !query.joins.is_empty()
            || query.derived.is_some()
            || !query.group_by.is_empty()
            || !query.grouping_sets.is_empty()
            || query.group_by_modifier != GroupByModifier::None
            || query.having.is_some()
            || query.limit.is_some()
            || query.offset.is_some()
            || !query.order_by.is_empty()
            || query.distinct
        {
            return None;
        }
        let table = &query.from[0];
        let schema = db.table(&table.name).ok()?;
        if schema.system
            || schema.row_security.enabled
            || !schema.table_type.eq_ignore_ascii_case("BASE TABLE")
            || schema
                .columns
                .iter()
                .any(|column| column.generated_expr.is_some())
            || table.system_time.is_some()
            || table.sample.is_some()
            || !table.column_aliases.is_empty()
        {
            return None;
        }
        let tables = vec![(table, schema.clone())];
        let [SelectItem::Expr {
            expr:
                Expr::Function {
                    name,
                    args,
                    distinct: false,
                },
            ..
        }] = query.projection.as_slice()
        else {
            return None;
        };
        if args.len() != 1
            || !matches!(
                name.to_ascii_uppercase().as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
            )
        {
            return None;
        }
        let argument = if name.eq_ignore_ascii_case("COUNT") && matches!(args[0], Expr::Wildcard) {
            Expr::Literal(Literal::Int(1))
        } else {
            let (_, col) = column(&args[0], &tables)?;
            if !exact(col) {
                return None;
            }
            args[0].clone()
        };
        if let Some(filter) = &query.selection {
            if !predicate(filter, &tables) {
                return None;
            }
            let mut indexed = false;
            crate::parser::visit::inspect_value_expr(
                filter,
                |expr| {
                    if let Some(((_, index), _)) = column(expr, &tables) {
                        indexed |= schema.indexes.iter().any(|key| {
                            key.columns.first().is_some_and(|col| {
                                col.eq_ignore_ascii_case(&schema.columns[index].name)
                            })
                        });
                    }
                    true
                },
                |_| {},
            );
            if indexed {
                return None;
            }
        }
        let argument = match &query.selection {
            Some(filter) => Expr::Case {
                operand: None,
                conditions: vec![filter.clone()].into(),
                results: vec![argument],
                else_result: Some(Box::new(Expr::Literal(Literal::Null))),
            },
            None => argument,
        };
        let aggregate = Expr::Function {
            name: name.clone(),
            args: vec![argument],
            distinct: false,
        };
        let mut source = query.clone();
        source.projection.clear();
        source.selection = None;
        Some((source, aggregate))
    }
    struct Group {
        source: Select,
        queries: Vec<Select>,
        aggregates: Vec<Expr>,
        consumers: usize,
        name: String,
    }
    let mut groups: Vec<Group> = Vec::new();
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            crate::parser::visit::inspect_value_expr(
                expr,
                |expr| {
                    if let Expr::Subquery(query) = expr {
                        if let Some((source, aggregate)) = candidate(db, query) {
                            if let Some(group) =
                                groups.iter_mut().find(|group| group.source == source)
                            {
                                group.consumers += 1;
                                if !group.queries.contains(query) {
                                    group.queries.push((**query).clone());
                                    group.aggregates.push(aggregate);
                                }
                            } else if groups.len() < 16 {
                                groups.push(Group {
                                    source,
                                    queries: vec![(**query).clone()],
                                    aggregates: vec![aggregate],
                                    consumers: 1,
                                    name: String::new(),
                                });
                            }
                        }
                        return false;
                    }
                    true
                },
                |_| {},
            );
        }
    }
    groups.retain(|group| group.queries.len() > 1 && group.queries.len() <= 64);
    if groups.is_empty() {
        return None;
    }
    let mut serial = 0;
    for group in &mut groups {
        loop {
            let name = format!("__mydb_scalar_agg_{serial}");
            serial += 1;
            if db.table(&name).is_err()
                && !select.from.iter().any(|table| {
                    table.name.eq_ignore_ascii_case(&name)
                        || table
                            .alias
                            .as_ref()
                            .is_some_and(|alias| alias.eq_ignore_ascii_case(&name))
                })
            {
                group.name = name;
                break;
            }
        }
    }
    let mut body = select.clone();
    for item in &mut body.projection {
        if let SelectItem::Expr { expr, .. } = item {
            crate::parser::visit::rewrite_value_expr(expr, |expr| {
                if let Expr::Subquery(query) = expr {
                    for group in &groups {
                        if let Some(index) = group
                            .queries
                            .iter()
                            .position(|original| original == query.as_ref())
                        {
                            query.table = Some(group.name.clone());
                            query.from[0].name = group.name.clone();
                            query.from[0].alias = None;
                            query.from[0].partitions.clear();
                            query.selection = None;
                            query.projection = vec![SelectItem::Expr {
                                expr: Expr::Identifier(format!("v{index}")),
                                alias: None,
                            }];
                            break;
                        }
                    }
                    return false;
                }
                true
            });
        }
    }
    let column_overrides = groups
        .iter()
        .map(|group| {
            group
                .queries
                .iter()
                .enumerate()
                .map(|(index, query)| {
                    let mut columns = describe_select_columns(db, query).ok()?;
                    if columns.len() != 1 {
                        return None;
                    }
                    columns[0].name = format!("v{index}");
                    columns.pop()
                })
                .collect::<Option<Vec<_>>>()
        })
        .collect::<Option<Vec<_>>>()?;
    Some(SharedAggregates {
        outer_probe,
        column_overrides,
        body,
        definitions: groups
            .into_iter()
            .map(|mut group| {
                group.source.projection = group
                    .aggregates
                    .into_iter()
                    .enumerate()
                    .map(|(index, expr)| SelectItem::Expr {
                        expr,
                        alias: Some(format!("v{index}")),
                    })
                    .collect();
                (
                    CteDef {
                        name: group.name,
                        columns: Vec::new(),
                        is_recursive: false,
                        query: QueryExpr::from(group.source),
                    },
                    group.consumers,
                )
            })
            .collect(),
    })
}

pub(super) fn factor(db: &Database, select: &Select) -> Option<SharedAggregates> {
    if let Some(shared) = factor_scalar(db, select) {
        return Some(shared);
    }
    // Ordinary point/range queries must not pay AST cloning or catalog lookups.
    if select.derived.is_none() && select.joins.iter().all(|join| join.derived.is_none()) {
        return None;
    }
    if !crate::util::env::flag("MYDB_SHARED_DERIVED_AGGREGATE", true)
        || crate::engine::params::streaming_param_source_active()
        || !super::super::cte::current_cte_registry().is_empty()
        || !safe_tree(select)
    {
        return None;
    }
    let mut body = select.clone();
    crate::planner_decorrelate::derived::pushdown_derived_predicates_with_db(db, &mut body);
    struct Candidate {
        key: Select,
        query: Select,
        count: usize,
        name: String,
    }
    fn visit(select: &Select, f: &mut impl FnMut(&Select)) {
        if let Some(QueryExpr::Select(inner)) = &select.derived {
            f(inner);
            visit(inner, f);
        }
        for join in &select.joins {
            if let Some(QueryExpr::Select(inner)) = &join.derived {
                f(inner);
                visit(inner, f);
            }
        }
    }
    let mut candidates: Vec<Candidate> = Vec::new();
    let mut names = HashSet::new();
    let mut names_for = |query: &Select| {
        for table in query
            .from
            .iter()
            .chain(query.joins.iter().map(|join| &join.table))
        {
            names.insert(
                table
                    .name
                    .rsplit('.')
                    .next()
                    .unwrap_or(&table.name)
                    .to_ascii_lowercase(),
            );
            if let Some(alias) = &table.alias {
                names.insert(alias.to_ascii_lowercase());
            }
        }
    };
    names_for(&body);
    visit(&body, &mut names_for);
    visit(&body, &mut |inner| {
        let Some(key) = identity(db, inner) else {
            return;
        };
        if let Some(candidate) = candidates.iter_mut().find(|candidate| candidate.key == key) {
            candidate.count += 1;
        } else if candidates.len() < 64 {
            candidates.push(Candidate {
                key,
                query: inner.clone(),
                count: 1,
                name: String::new(),
            });
        }
    });
    candidates.retain(|candidate| candidate.count > 1);
    if candidates.is_empty() {
        return None;
    }
    let mut serial = 0;
    for candidate in &mut candidates {
        loop {
            let name = format!("__mydb_shared_agg_{serial}");
            serial += 1;
            if names.insert(name.clone()) {
                candidate.name = name;
                break;
            }
        }
    }
    fn replace(db: &Database, select: &mut Select, candidates: &[Candidate]) {
        if let Some(QueryExpr::Select(inner)) = &mut select.derived {
            if let Some(candidate) = identity(db, inner)
                .and_then(|key| candidates.iter().find(|candidate| candidate.key == key))
            {
                if let Some(table) = select.from.first_mut() {
                    table.alias = Some(table.alias.clone().unwrap_or_else(|| table.name.clone()));
                    table.name = candidate.name.clone();
                    select.table = Some(candidate.name.clone());
                    select.derived = None;
                }
            } else {
                replace(db, inner, candidates);
            }
        }
        for join in &mut select.joins {
            if let Some(QueryExpr::Select(inner)) = &mut join.derived {
                if let Some(candidate) = identity(db, inner)
                    .and_then(|key| candidates.iter().find(|candidate| candidate.key == key))
                {
                    join.table.alias = Some(
                        join.table
                            .alias
                            .clone()
                            .unwrap_or_else(|| join.table.name.clone()),
                    );
                    join.table.name = candidate.name.clone();
                    join.derived = None;
                } else {
                    replace(db, inner, candidates);
                }
            }
        }
    }
    replace(db, &mut body, &candidates);
    Some(SharedAggregates {
        outer_probe: None,
        column_overrides: Vec::new(),
        body,
        definitions: candidates
            .into_iter()
            .map(|candidate| {
                (
                    CteDef {
                        name: candidate.name,
                        columns: Vec::new(),
                        is_recursive: false,
                        query: QueryExpr::from(candidate.query),
                    },
                    candidate.count,
                )
            })
            .collect(),
    })
}
