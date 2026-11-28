use super::storage::{
    adjacency_batch_size, AdjacencyBounds, ElementRef, GraphStore, LabeledNeighbor, Neighbor,
    ADJACENCY_WINDOW_SIZE,
};
use super::values::{self, eval, Bindings, Bound, Path};
use super::*;
use crate::graph::{Change, Element, Operator, PathMode, Pattern, Projection, Step};
use serde_json::{Map, Value as Json};
use smallvec::{smallvec, SmallVec};
use std::borrow::Cow;

pub(super) fn execute(
    store: &mut GraphStore<'_>,
    program: Program,
) -> Result<QueryResponse, QueryError> {
    let columns = program_columns(&program);
    let mut rows = vec![Bindings::new()];
    let mut affected = 0u64;
    for clause in &program.clauses {
        crate::engine::cancel::check_cancelled()?;
        match clause {
            Clause::For(name, list) => {
                let mut next = Vec::new();
                for row in &rows {
                    if row.contains_key(name) {
                        return Err(invalid(format!("Duplicate GQL binding '{name}'")));
                    }
                    for expression in list {
                        let value = eval(store, &program.atoms, row, expression)?;
                        reserve_binding(store, row)?;
                        let mut row = row.clone();
                        row.insert(name, Bound::Scalar(value));
                        next.push(row);
                    }
                }
                rows = next;
            }
            Clause::Match {
                optional,
                patterns,
                predicate,
            } => {
                rows = match_clause(
                    store,
                    &program.atoms,
                    rows,
                    patterns,
                    predicate.as_ref(),
                    *optional,
                )?;
            }
            Clause::Insert(patterns) => {
                for row in &mut rows {
                    for pattern in patterns {
                        let (mut node, inserted) =
                            insert_node(store, &program.atoms, row, &pattern.start)?;
                        affected += u64::from(inserted);
                        for step in &pattern.steps {
                            let (next, inserted) =
                                insert_node(store, &program.atoms, row, &step.node)?;
                            let props = properties(store, &program.atoms, row, &step.edge)?;
                            let edge = store.insert_edge(
                                node,
                                next,
                                step.direction,
                                &step.edge.labels,
                                props,
                            )?;
                            bind_inserted(row, &step.edge, edge)?;
                            affected += 1 + u64::from(inserted);
                            node = next;
                        }
                    }
                }
            }
            Clause::Set(changes) | Clause::Remove(changes) => {
                let set = matches!(clause, Clause::Set(_));
                for row in &rows {
                    for change in changes {
                        let (variable, key, expression, label) = match change {
                            Change::Property {
                                variable,
                                key,
                                value,
                            } => (variable, key, value.as_ref(), false),
                            Change::Label { variable, label } => (variable, label, None, true),
                        };
                        match values::binding(row, variable)? {
                            Bound::Null => continue,
                            Bound::Element(entry) => {
                                let value = if label && set {
                                    Some(Json::Bool(true))
                                } else {
                                    expression
                                        .map(|expr| eval(store, &program.atoms, row, expr))
                                        .transpose()?
                                };
                                check_scalar(value.as_ref())?;
                                store.change(*entry, key, value, label)?;
                                affected += 1;
                            }
                            _ => {
                                return Err(invalid(
                                    "SET and REMOVE require node or edge variables",
                                ))
                            }
                        }
                    }
                }
            }
            Clause::Delete { detach, variables } => {
                let mut elements = HashSet::new();
                for row in &rows {
                    for variable in variables {
                        match values::binding(row, variable)? {
                            Bound::Null => (),
                            Bound::Element(entry) => {
                                elements.insert(*entry);
                            }
                            _ => return Err(invalid("DELETE requires node or edge variables")),
                        }
                    }
                }
                // Explicitly selected edges are removed before their endpoints.
                let mut elements: Vec<_> = elements.into_iter().collect();
                elements.sort_unstable_by_key(|entry| (entry.node(), entry.id));
                for entry in elements {
                    if store.delete(entry, *detach)? {
                        affected += 1;
                    }
                }
            }
        }
    }

    let Some(projection) = program.result else {
        return Ok(QueryResponse::OkAffected {
            affected_rows: affected,
        });
    };
    let limit = projection
        .limit
        .as_ref()
        .map(|expr| {
            eval(store, &program.atoms, &Bindings::new(), expr)?
                .as_u64()
                .ok_or_else(|| invalid("GQL LIMIT requires a nonnegative integer"))
        })
        .transpose()?
        .unwrap_or(u64::MAX);
    if projection
        .items
        .iter()
        .any(|(expr, _)| matches!(expr, Expression::Count(_)))
    {
        return grouped_result(store, &program.atoms, &projection, columns, rows, limit);
    }
    if projection.order.is_empty() && !projection.distinct {
        rows.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    }
    let wire_order = result_integer_order(&projection, &columns);
    let input = GraphProjection {
        atoms: &program.atoms,
        projection: &projection,
        columns: &columns,
        limit,
        wire_order: wire_order.is_some(),
    };
    if !projection.distinct
        && wire_order.is_none()
        && !projection.order.is_empty()
        && limit > 0
        && limit < (rows.len() / 2) as u64
    {
        let winners = input.top_k(store, &rows, limit as usize)?;
        let result = input
            .project_slice(store, &winners)?
            .into_iter()
            .map(|(row, _)| row)
            .collect();
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns,
            rows: result,
        }));
    }
    if let Some((0, ascending)) = wire_order {
        if !projection.distinct
            && matches!(columns.as_slice(), [c] if c.column_type == ColumnType::LongLong)
        {
            let mut integers =
                input.project(store, &rows, GraphProjection::project_integer_slice)?;
            integers.sort_unstable_by_key(|cell| (cell.is_none(), cell.unwrap_or_default()));
            if !ascending {
                integers.reverse();
            }
            integers.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
            store.charge(
                integers.len() * super::super::mem_budget::estimate_row_bytes(&[Value::Null]),
            )?;
            let rows = integers
                .into_iter()
                .map(|cell| vec![cell.map_or(Value::Null, Value::Int)])
                .collect();
            return Ok(QueryResponse::ResultSet(ResultSet { columns, rows }));
        }
    }
    let mut result = input.project(store, &rows, GraphProjection::project_slice)?;
    let mut result = if let Some((column, asc)) = wire_order {
        let mut wire: Vec<_> = result.into_iter().map(|(row, _)| row).collect();
        let keys = wire.iter().map(|row| row[column].clone()).collect();
        super::super::sort::sort_by_precomputed_single_key(
            &mut wire,
            super::super::sort::SortField {
                ascending: asc,
                nulls_first: !asc,
                collation_id: None,
            },
            keys,
        );
        wire
    } else {
        if !projection.order.is_empty() {
            result.sort_by(|(_, a), (_, b)| {
                a.iter()
                    .zip(b)
                    .zip(&projection.order)
                    .find_map(|((a, b), (_, desc))| {
                        let cmp = values::sort_cmp(a, b);
                        (cmp != Ordering::Equal).then(|| if *desc { cmp.reverse() } else { cmp })
                    })
                    .unwrap_or(Ordering::Equal)
            });
        }
        result.into_iter().map(|(row, _)| row).collect()
    };
    result.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    Ok(QueryResponse::ResultSet(ResultSet {
        columns,
        rows: result,
    }))
}

fn grouped_result(
    store: &mut GraphStore<'_>,
    atoms: &[Expr],
    projection: &Projection,
    columns: Vec<Column>,
    rows: Vec<Bindings<'_>>,
    limit: u64,
) -> Result<QueryResponse, QueryError> {
    let mut groups = Vec::<Vec<Json>>::new();
    let mut indices = crate::util::fast_hash::ah_hash_map();
    let mut variables = SmallVec::new();
    let mut fields = SmallVec::new();
    let mut counts = Vec::new();
    let mut keys = Vec::new();
    for (slot, (expr, _)) in projection.items.iter().enumerate() {
        if let Expression::Count(argument) = expr {
            counts.push((slot, argument.as_deref()));
            if let Some(arg) = argument {
                collect_properties(arg, &mut variables, &mut fields);
            }
        } else {
            keys.push((slot, expr));
            collect_properties(expr, &mut variables, &mut fields);
        }
    }
    let empty = vec![Json::from(0); columns.len()];
    if keys.is_empty() {
        groups.push(empty.clone());
    }
    for window in rows.chunks(512) {
        crate::engine::cancel::check_cancelled()?;
        preload_projection_batch(store, window, &variables, &fields)?;
        for row in window {
            let index = if keys.is_empty() {
                0
            } else {
                let key = keys
                    .iter()
                    .map(|(_, expr)| eval(store, atoms, row, expr))
                    .collect::<Result<Vec<_>, _>>()?;
                let normalized: Vec<_> = key.iter().map(values::distinct_value).collect();
                let bytes = serde_json::to_vec(&normalized)
                    .map_err(|_| invalid("Cannot encode graph grouping key"))?;
                if let Some(index) = indices.get(&bytes) {
                    *index
                } else {
                    store.charge(
                        bytes.capacity()
                            + 96
                            + empty.len() * std::mem::size_of::<Json>()
                            + key.iter().map(json_size).sum::<usize>(),
                    )?;
                    let index = groups.len();
                    indices.insert(bytes, index);
                    let mut values = empty.clone();
                    for ((slot, _), value) in keys.iter().zip(key) {
                        values[*slot] = value;
                    }
                    groups.push(values);
                    index
                }
            };
            let group = &mut groups[index];
            for &(slot, arg) in &counts {
                let present = match arg {
                    None => true,
                    Some(Expression::Variable(name)) => !matches!(
                        values::binding(row, name)?,
                        Bound::Null | Bound::Scalar(Json::Null)
                    ),
                    Some(expr) => !eval(store, atoms, row, expr)?.is_null(),
                };
                if present {
                    group[slot] = Json::from(group[slot].as_i64().unwrap() + 1);
                }
            }
        }
    }
    let order = projection
        .order
        .iter()
        .map(|(expr, desc)| {
            let alias = if let Expression::Variable(name) = expr {
                projection.items.iter().position(|(_, a)| a == name)
            } else {
                None
            };
            alias
                .or_else(|| projection.items.iter().position(|(item, _)| item == expr))
                .map(|slot| (slot, *desc))
                .ok_or_else(|| invalid("Invalid grouped ORDER BY"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !order.is_empty() {
        groups.sort_by(|a, b| {
            for &(slot, desc) in &order {
                let ordering = values::sort_cmp(&a[slot], &b[slot]);
                if ordering != Ordering::Equal {
                    return if desc { ordering.reverse() } else { ordering };
                }
            }
            Ordering::Equal
        });
    }
    groups.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    let rows = groups
        .into_iter()
        .map(|group| {
            group
                .into_iter()
                .zip(&columns)
                .map(|(value, col)| values::wire(value, &col.column_type))
                .collect()
        })
        .collect();
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

fn result_integer_order(projection: &Projection, columns: &[Column]) -> Option<(usize, bool)> {
    let [(Expression::Variable(alias), descending)] = projection.order.as_slice() else {
        return None;
    };
    let slot = projection
        .items
        .iter()
        .position(|(_, name)| name == alias)?;
    matches!(
        columns[slot].column_type,
        ColumnType::LongLong | ColumnType::Tiny
    )
    .then_some((slot, !descending))
}

type GraphResultRows = Vec<(Vec<Value>, Vec<Json>)>;

struct GraphProjection<'a> {
    atoms: &'a [Expr],
    projection: &'a Projection,
    columns: &'a [Column],
    limit: u64,
    wire_order: bool,
}

impl GraphProjection<'_> {
    fn top_k<'p>(
        &self,
        store: &mut GraphStore<'_>,
        rows: &[Bindings<'p>],
        k: usize,
    ) -> Result<Vec<Bindings<'p>>, QueryError> {
        let order: Vec<_> = self
            .projection
            .order
            .iter()
            .map(|(expr, desc)| {
                let source = match expr {
                    Expression::Variable(name) => self
                        .projection
                        .items
                        .iter()
                        .find(|(_, alias)| alias == name)
                        .map(|(value, _)| value)
                        .unwrap_or(expr),
                    _ => expr,
                };
                (source, *desc)
            })
            .collect();
        let mut variables = SmallVec::new();
        let mut fields = SmallVec::new();
        for (expr, _) in &order {
            collect_properties(expr, &mut variables, &mut fields);
        }
        let compare = |a: &(usize, Vec<Json>, usize), b: &(usize, Vec<Json>, usize)| {
            for ((a, b), (_, desc)) in a.1.iter().zip(&b.1).zip(&order) {
                let cmp = values::sort_cmp(a, b);
                if cmp != Ordering::Equal {
                    return if *desc { cmp.reverse() } else { cmp };
                }
            }
            a.0.cmp(&b.0)
        };
        let mut scratch = super::super::mem_budget::ScopedCharge::for_budget(
            super::super::mem_budget::current_budget(),
        );
        let capacity = k * 2;
        scratch.add(capacity * std::mem::size_of::<(usize, Vec<Json>, usize)>())?;
        let mut best = Vec::with_capacity(capacity);
        let mut validated = vec![None; self.projection.items.len()];
        for (batch, window) in rows.chunks(512).enumerate() {
            crate::engine::cancel::check_cancelled()?;
            preload_projection_batch(store, window, &variables, &fields)?;
            for (slot, row) in window.iter().enumerate() {
                // Preserve errors in expressions on rows discarded by LIMIT. A graph
                // element property is a scalar, so converting it to text is total.
                for (last, (expr, _)) in validated.iter_mut().zip(&self.projection.items) {
                    let (source, total_cast) = match expr {
                        Expression::Cast(inner, kind) => (inner.as_ref(), *kind == Cast::String),
                        other => (other, true),
                    };
                    if let Expression::Property(name, _) = source {
                        match row.get(name) {
                            Some(Bound::Element(entry)) => {
                                if total_cast || *last == Some(*entry) {
                                    continue;
                                }
                                eval(store, self.atoms, row, expr)?;
                                *last = Some(*entry);
                                continue;
                            }
                            Some(Bound::Null) => continue,
                            _ => (),
                        }
                    }
                    eval(store, self.atoms, row, expr)?;
                }
                let keys = order
                    .iter()
                    .map(|(key, _)| eval(store, self.atoms, row, key))
                    .collect::<Result<Vec<_>, _>>()?;
                let bytes = keys.iter().map(json_size).sum();
                scratch.add(bytes)?;
                best.push((batch * 512 + slot, keys, bytes));
                if best.len() == capacity {
                    best.select_nth_unstable_by(k, compare);
                    let freed = best.drain(k..).map(|(_, _, bytes)| bytes).sum();
                    scratch.release_bytes(freed);
                }
            }
        }
        best.sort_unstable_by(compare);
        Ok(best
            .into_iter()
            .take(k)
            .map(|(index, _, _)| rows[index].clone())
            .collect())
    }

    fn project<'p, R: Send>(
        &self,
        store: &mut GraphStore<'_>,
        rows: &[Bindings<'p>],
        project_slice: impl Fn(&Self, &mut GraphStore<'_>, &[Bindings<'p>]) -> Result<Vec<R>, QueryError>
            + Copy
            + Send,
    ) -> Result<Vec<R>, QueryError> {
        if rows.len() < 4096 || self.projection.distinct || self.projection.order.is_empty() {
            return project_slice(self, store, rows);
        }
        let Some(worker) = try_graph_read_worker(store.txn) else {
            return project_slice(self, store, rows);
        };
        let context = super::super::select::execution_context::ExecutionContext::capture(store.txn);
        let (db, table) = store.projection_source();
        let txn = store.txn;
        let middle = (rows.len() / 2).div_ceil(1024) * 1024;
        let (left, right) = rows.split_at(middle);
        crate::util::thread_spawn::scope(|scope| {
            let task = crate::util::thread_spawn::Builder::new()
                .stack_size(2 * 1024 * 1024)
                .spawn_scoped(scope, move || {
                    let _lease = worker;
                    context.run(db, || {
                        let mut store = GraphStore::new(db, table, txn)?;
                        let result = project_slice(self, &mut store, right)?;
                        Ok::<_, QueryError>((result, store.into_charge()))
                    })
                });
            let Ok(task) = task else {
                return project_slice(self, store, rows);
            };
            let mut result = project_slice(self, store, left)?;
            let (mut right, charge) = task
                .join()
                .map_err(|_| invalid("Graph projection worker failed"))??;
            store.retain_worker_charge(charge);
            result.append(&mut right);
            Ok(result)
        })
    }

    // Sort a single integral result in a contiguous vector before creating
    // protocol rows. Equal values have identical wire representations.
    fn project_integer_slice(
        &self,
        store: &mut GraphStore<'_>,
        rows: &[Bindings<'_>],
    ) -> Result<Vec<Option<i64>>, QueryError> {
        let expression = &self.projection.items[0].0;
        let mut variables = SmallVec::new();
        let mut fields = SmallVec::new();
        collect_properties(expression, &mut variables, &mut fields);
        store.charge(rows.len() * std::mem::size_of::<Option<i64>>())?;
        let mut values = Vec::with_capacity(rows.len());
        let columns_per_window = if variables.len() == 1 && fields.len() == 1 {
            8192
        } else {
            1024
        };
        for window in rows.chunks(columns_per_window) {
            crate::engine::cancel::check_cancelled()?;
            preload_projection_batch(store, window, &variables, &fields)?;
            for row in window {
                let value = eval(store, self.atoms, row, expression)?;
                values.push(value.as_i64());
            }
        }
        Ok(values)
    }

    fn project_slice(
        &self,
        store: &mut GraphStore<'_>,
        rows: &[Bindings<'_>],
    ) -> Result<GraphResultRows, QueryError> {
        let Self {
            atoms,
            projection,
            columns,
            limit,
            wire_order,
        } = *self;
        let mut result = Vec::with_capacity(rows.len().min(limit as usize).min(1024));
        let mut seen = HashSet::new();
        let mut output_bytes = 0usize;
        let mut property_variables = SmallVec::<[&str; 4]>::new();
        let mut property_fields = SmallVec::<[&str; 4]>::new();
        for expr in projection
            .items
            .iter()
            .map(|(expr, _)| expr)
            .chain(projection.order.iter().map(|(expr, _)| expr))
        {
            collect_properties(expr, &mut property_variables, &mut property_fields);
        }
        // Scalar projection can sort and deduplicate a wider set of heap
        // addresses within the existing per-statement projection allowance.
        // This keeps a heap page shared by many endpoints in one read batch.
        let window_rows = if !projection.distinct
            && property_fields.len() == 1
            && property_variables.len() == 1
        {
            8192
        } else {
            1024
        };
        'windows: for window in rows.chunks(window_rows) {
            if projection.order.is_empty() && result.len() as u64 >= limit {
                break;
            }
            preload_projection_batch(store, window, &property_variables, &property_fields)?;
            for row in window {
                if projection.order.is_empty() && result.len() as u64 >= limit {
                    break 'windows;
                }
                let (output, order) = if !projection.distinct
                    && (wire_order || projection.order.is_empty())
                {
                    let output = projection
                        .items
                        .iter()
                        .zip(columns)
                        .map(|((expr, _), column)| {
                            Ok(values::wire(
                                eval(store, atoms, row, expr)?,
                                &column.column_type,
                            ))
                        })
                        .collect::<Result<Vec<_>, QueryError>>()?;
                    (output, Vec::new())
                } else {
                    let projected = projection
                        .items
                        .iter()
                        .map(|(expr, _)| eval(store, atoms, row, expr))
                        .collect::<Result<Vec<_>, _>>()?;
                    if projection.distinct
                        && !seen.insert(
                            serde_json::to_vec(
                                &projected
                                    .iter()
                                    .map(values::distinct_value)
                                    .collect::<Vec<_>>(),
                            )
                            .map_err(|_| invalid("Cannot encode DISTINCT graph result"))?,
                        )
                    {
                        continue;
                    }
                    let order = if wire_order {
                        Vec::new()
                    } else {
                        projection
                            .order
                            .iter()
                            .map(|(expr, _)| {
                                if let Expression::Variable(alias) = expr {
                                    if let Some(index) =
                                        projection.items.iter().position(|(_, name)| name == alias)
                                    {
                                        return Ok(projected[index].clone());
                                    }
                                }
                                eval(store, atoms, row, expr)
                            })
                            .collect::<Result<Vec<_>, _>>()?
                    };
                    let output = projected
                        .into_iter()
                        .zip(columns)
                        .map(|(value, col)| values::wire(value, &col.column_type))
                        .collect::<Vec<_>>();

                    (output, order)
                };
                output_bytes += super::super::mem_budget::estimate_row_bytes(&output)
                    + order.iter().map(json_size).sum::<usize>();
                if output_bytes >= 32 * 1024 {
                    store.charge(std::mem::take(&mut output_bytes))?;
                }
                result.push((output, order));
            }
        }

        store.charge(output_bytes)?;
        Ok(result)
    }
}

fn preload_projection_batch(
    store: &mut GraphStore<'_>,
    rows: &[Bindings<'_>],
    variables: &[&str],
    fields: &[&str],
) -> Result<(), QueryError> {
    if variables.is_empty() {
        return Ok(());
    }
    let ids = rows
        .iter()
        .flat_map(|row| {
            variables.iter().filter_map(|name| match row.get(name) {
                Some(Bound::Element(entry)) if !store.projection_has_all(entry.row, fields) => {
                    Some(entry.row)
                }
                _ => None,
            })
        })
        .collect();
    let field = match fields {
        [field] => Some(*field),
        _ => None,
    };
    store.preload_projection(ids, field)
}

fn collect_properties<'a>(
    expr: &'a Expression,
    variables: &mut SmallVec<[&'a str; 4]>,
    fields: &mut SmallVec<[&'a str; 4]>,
) {
    match expr {
        Expression::Property(variable, field) => {
            if !variables.contains(&variable.as_str()) {
                variables.push(variable);
            }
            if !fields.contains(&field.as_str()) {
                fields.push(field);
            }
        }
        Expression::Cast(expr, _) | Expression::Not(expr) | Expression::IsNull(expr, _) => {
            collect_properties(expr, variables, fields);
        }
        Expression::Binary(left, _, right) => {
            collect_properties(left, variables, fields);
            collect_properties(right, variables, fields);
        }
        Expression::Record(items) => {
            for (_, expr) in items {
                collect_properties(expr, variables, fields);
            }
        }
        _ => (),
    }
}

fn json_size(value: &Json) -> usize {
    std::mem::size_of::<Json>()
        + match value {
            Json::String(value) => value.len(),
            Json::Array(values) => values.iter().map(json_size).sum(),
            Json::Object(values) => values
                .iter()
                .map(|(key, value)| key.len() + json_size(value))
                .sum(),
            _ => 0,
        }
}

fn reserve_binding(store: &mut GraphStore<'_>, row: &Bindings<'_>) -> Result<(), QueryError> {
    let bytes = row
        .iter()
        .map(|(_, binding)| {
            std::mem::size_of::<(&str, Bound)>()
                + match binding {
                    Bound::Scalar(value) => json_size(value),
                    Bound::Group(elements) => std::mem::size_of_val(elements.as_slice()),
                    Bound::Path(path) => {
                        std::mem::size_of_val(path.edges.as_slice())
                            + std::mem::size_of_val(path.nodes.as_slice())
                    }
                    _ => 0,
                }
        })
        .sum::<usize>();
    store.charge(bytes + 128)
}

fn match_clause<'p>(
    store: &mut GraphStore<'_>,
    atoms: &[Expr],
    rows: Vec<Bindings<'p>>,
    patterns: &'p [Pattern],
    predicate: Option<&Expression>,
    optional: bool,
) -> Result<Vec<Bindings<'p>>, QueryError> {
    if let [pattern] = patterns {
        if optional && rows.len() >= 16 && predicate.is_none() {
            if let Some(result) = join_bound_walk(store, atoms, &rows, pattern)? {
                return Ok(result);
            }
        }
        if !optional
            && (rows.len() >= 16
                || pattern.steps.len() > 1
                || (store.has_labeled_adjacency()
                    && pattern
                        .steps
                        .first()
                        .is_some_and(|step| covers_edge(&step.edge))))
            && pattern.mode == PathMode::Walk
            && !pattern.shortest
            && pattern.path.is_none()
            && !pattern.steps.is_empty()
            && pattern
                .steps
                .iter()
                .any(|step| !step.edge.labels.is_empty() || !step.node.labels.is_empty())
            && pattern.steps.iter().all(|step| !step.quantified)
        {
            return match_fixed_walk(store, atoms, rows, pattern, predicate);
        }
    }
    let mut out = Vec::new();
    for outer in rows {
        let mut found = vec![outer.clone()];
        for pattern in patterns {
            let mut inner = Vec::new();
            for row in found {
                let local_predicate = (patterns.len() == 1).then_some(predicate).flatten();
                inner.extend(match_pattern(store, atoms, row, pattern, local_predicate)?);
            }
            found = inner;
        }
        let start = out.len();
        for row in found {
            if patterns.len() > 1 && !passes(store, atoms, &row, predicate)? {
                continue;
            }
            out.push(row);
        }
        if optional && out.len() == start {
            let mut fallback = outer;
            for pattern in patterns {
                optional_bind(&mut fallback, pattern.path.as_ref());
                optional_bind(&mut fallback, pattern.start.variable.as_ref());
                for step in &pattern.steps {
                    optional_bind(&mut fallback, step.edge.variable.as_ref());
                    optional_bind(&mut fallback, step.node.variable.as_ref());
                }
            }
            reserve_binding(store, &fallback)?;
            out.push(fallback);
        }
    }
    Ok(out)
}

// Share a fixed traversal across outer rows bound to different endpoints.
fn join_bound_walk<'p>(
    store: &mut GraphStore<'_>,
    atoms: &[Expr],
    rows: &[Bindings<'p>],
    pattern: &'p Pattern,
) -> Result<Option<Vec<Bindings<'p>>>, QueryError> {
    let Some(start) = pattern.start.variable.as_deref() else {
        return Ok(None);
    };
    let Some(end) = pattern
        .steps
        .last()
        .and_then(|s| s.node.variable.as_deref())
    else {
        return Ok(None);
    };
    let independent =
        |element: &Element| element.properties.is_empty() && element.predicate.is_none();
    if start == end
        || pattern.shortest
        || pattern.path.is_some()
        || pattern.mode != PathMode::Walk
        || !independent(&pattern.start)
        || !pattern
            .steps
            .iter()
            .all(|s| !s.quantified && independent(&s.edge) && independent(&s.node))
    {
        return Ok(None);
    }
    let locals: Vec<_> = pattern
        .steps
        .iter()
        .flat_map(|s| [s.edge.variable.as_deref(), s.node.variable.as_deref()])
        .flatten()
        .filter(|v| *v != start && *v != end)
        .collect();
    let mut anchors = crate::util::fast_hash::ah_hash_set();
    let mut seeds = Vec::new();
    let mut requested = crate::util::fast_hash::ah_hash_set();
    for row in rows {
        let (Some(Bound::Element(a)), Some(Bound::Element(b))) = (row.get(start), row.get(end))
        else {
            return Ok(None);
        };
        if locals.iter().any(|v| row.contains_key(v)) {
            return Ok(None);
        }
        requested.insert((a.id, b.id));
        if anchors.insert(a.id) {
            let mut seed = Bindings::new();
            seed.insert(start, Bound::Element(*a));
            seeds.push(seed);
        }
    }
    if seeds.len() > rows.len() / 2 {
        return Ok(None);
    }
    store.charge(requested.len() * 64 + seeds.len() * 128)?;
    let mut matches = crate::util::fast_hash::ah_hash_map::<(u64, u64), Vec<Bindings<'p>>>();
    for found in match_fixed_walk(store, atoms, seeds, pattern, None)? {
        if let (Some(Bound::Element(a)), Some(Bound::Element(b))) =
            (found.get(start), found.get(end))
        {
            let pair = (a.id, b.id);
            if requested.contains(&pair) {
                matches.entry(pair).or_default().push(found);
            }
        }
    }
    let mut out = Vec::new();
    for row in rows {
        let (Some(Bound::Element(a)), Some(Bound::Element(b))) = (row.get(start), row.get(end))
        else {
            unreachable!()
        };
        if let Some(found) = matches.get(&(a.id, b.id)) {
            for inner in found {
                let mut merged = row.clone();
                for (name, value) in inner.iter() {
                    if !merged.contains_key(name) {
                        merged.insert(name, value.clone());
                    }
                }
                reserve_binding(store, &merged)?;
                out.push(merged);
            }
        } else {
            let mut merged = row.clone();
            for name in &locals {
                if !merged.contains_key(name) {
                    merged.insert(name, Bound::Null);
                }
            }
            reserve_binding(store, &merged)?;
            out.push(merged);
        }
    }
    Ok(Some(out))
}

fn match_fixed_walk<'p>(
    store: &mut GraphStore<'_>,
    atoms: &[Expr],
    rows: Vec<Bindings<'p>>,
    pattern: &'p Pattern,
    predicate: Option<&Expression>,
) -> Result<Vec<Bindings<'p>>, QueryError> {
    let mut frontier = Vec::new();
    for row in rows {
        let (starts, proof) = start_nodes(store, atoms, &row, &pattern.start, predicate)?;
        for start in starts {
            if let Some(row) =
                match_element_with_proof(store, atoms, &row, &pattern.start, start, proof)?
            {
                reserve_binding(store, &row)?;
                frontier.push((start.id, row.into_owned()));
            }
        }
    }
    for step in &pattern.steps {
        frontier = expand_fixed_vertices(store, atoms, &frontier, step)?;
        if frontier.is_empty() {
            break;
        }
    }
    let mut out = Vec::new();
    for (_, row) in frontier {
        if passes(store, atoms, &row, predicate)? {
            out.push(row);
        }
    }
    Ok(out)
}

fn predicate_projection(element: &Element) -> Option<Option<&str>> {
    let variable = element.variable.as_deref()?;
    let mut variables = SmallVec::new();
    let mut fields = SmallVec::new();
    collect_properties(element.predicate.as_ref()?, &mut variables, &mut fields);
    variables
        .contains(&variable)
        .then(|| match fields.as_slice() {
            [field] => Some(*field),
            _ => None,
        })
}

fn covers_edge(edge: &Element) -> bool {
    !edge.labels.is_empty()
}

fn expand_fixed_vertices<'p>(
    store: &mut GraphStore<'_>,
    atoms: &[Expr],
    starts: &[(u64, Bindings<'p>)],
    step: &'p Step,
) -> Result<Vec<(u64, Bindings<'p>)>, QueryError> {
    let edge_label = step.edge.labels.first();
    if covers_edge(&step.edge) && store.has_labeled_adjacency() {
        return expand_labeled_vertices(store, atoms, starts, step);
    }
    let check_edge = step.edge.variable.is_some()
        || step.edge.labels.len() > 1
        || !step.edge.properties.is_empty()
        || step.edge.predicate.is_some();
    let node_label = step.node.labels.first();
    let edge_field = predicate_projection(&step.edge);
    let node_field = predicate_projection(&step.node);
    let edge_proof = ElementProof {
        label: edge_label.map(|_| 0),
        property: None,
    };
    let bounds = if starts.len() >= 16 {
        let Some(bounds) = store.adjacency_bounds(edge_label, node_label)? else {
            return Ok(Vec::new());
        };
        bounds
    } else {
        AdjacencyBounds::default()
    };
    let batch_size = adjacency_batch_size(store.txn);
    let mut out = Vec::new();
    for roots in starts.chunks(ADJACENCY_WINDOW_SIZE) {
        let anchors: Vec<_> = roots.iter().map(|(id, _)| *id).collect();
        let batches = store.adjacent_window(&anchors, step.direction, false, batch_size, bounds)?;
        for (roots, batch) in roots.chunks(batch_size).zip(batches) {
            if let Some(batch) = &batch {
                store.account_adjacency_batch(batch)?;
            }
            let mut candidates = Vec::new();
            let mut scratch = super::super::mem_budget::ScopedCharge::for_budget(
                super::super::mem_budget::current_budget(),
            );
            for (id, row) in roots {
                let single;
                let neighbors = if let Some(batch) = &batch {
                    batch.neighbors(*id)
                } else {
                    single = store.adjacent_bounded(*id, step.direction, bounds)?;
                    single.as_slice()
                };
                scratch.add(std::mem::size_of::<(&Bindings<'_>, Neighbor)>() * neighbors.len())?;
                candidates.extend(neighbors.iter().map(|neighbor| (row, *neighbor)));
            }
            for window in candidates.chunks(1024) {
                let edges = edge_label
                    .map(|label| {
                        store.label_members(
                            false,
                            label,
                            window.iter().map(|(_, edge)| edge.edge.id).collect(),
                        )
                    })
                    .transpose()?;
                let has_edge_label = |edge: &Neighbor| {
                    edges
                        .as_ref()
                        .is_none_or(|members| members.contains(&edge.edge.id))
                };
                if let Some(field) = edge_field.filter(|_| window.len() >= 16) {
                    let rows = window
                        .iter()
                        .filter(|(_, edge)| has_edge_label(edge))
                        .map(|(_, edge)| edge.edge.row)
                        .collect();
                    store.preload_projection(rows, field)?;
                }
                let mut endpoints = Vec::new();
                for &(row, edge) in window {
                    if !has_edge_label(&edge) {
                        continue;
                    }
                    let row = if check_edge {
                        let Some(row) = match_element_with_proof(
                            store, atoms, row, &step.edge, edge.edge, edge_proof,
                        )?
                        else {
                            continue;
                        };
                        row
                    } else {
                        Cow::Borrowed(row)
                    };
                    endpoints.push((edge.node, row));
                }
                match_fixed_targets(store, atoms, &step.node, node_field, endpoints, &mut out)?;
            }
        }
    }
    if edge_field.is_some() || node_field.is_some() {
        store.clear_projection();
    }
    Ok(out)
}

fn expand_labeled_vertices<'p>(
    store: &mut GraphStore<'_>,
    atoms: &[Expr],
    starts: &[(u64, Bindings<'p>)],
    step: &'p Step,
) -> Result<Vec<(u64, Bindings<'p>)>, QueryError> {
    let node_field = predicate_projection(&step.node);
    let bounds = if starts.len() >= 16 {
        let Some(bounds) = store.adjacency_bounds(None, step.node.labels.first())? else {
            return Ok(Vec::new());
        };
        bounds
    } else {
        AdjacencyBounds::default()
    };
    let mut out = Vec::new();
    let (label, additional_labels) = step.edge.labels.split_first().expect("covering edge label");
    let batch_size = adjacency_batch_size(store.txn);
    for roots in starts.chunks(batch_size) {
        let anchors = roots.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let neighbors = store.labeled_adjacency(&anchors, step.direction, label, bounds)?;
        let mut scratch = super::super::mem_budget::ScopedCharge::for_budget(
            super::super::mem_budget::current_budget(),
        );
        let mut candidates = Vec::new();
        for (id, row) in roots {
            if let Some(neighbors) = neighbors.get(id) {
                scratch.add(
                    neighbors.len() * std::mem::size_of::<(LabeledNeighbor, &Bindings<'_>)>(),
                )?;
                candidates.extend(neighbors.iter().map(|edge| (*edge, row)));
            }
        }
        for window in candidates.chunks_mut(1024) {
            let mut end = window.len();
            for label in additional_labels {
                let members = store.label_members(
                    false,
                    label,
                    window[..end].iter().map(|(edge, _)| edge.edge).collect(),
                )?;
                let mut keep = 0;
                for i in 0..end {
                    if members.contains(&window[i].0.edge) {
                        window[keep] = window[i];
                        keep += 1;
                    }
                }
                end = keep;
            }
            let edges = &window[..end];
            let needs_edge = step.edge.variable.is_some()
                || !step.edge.properties.is_empty()
                || step.edge.predicate.is_some();
            let endpoints = if needs_edge {
                let ids = edges.iter().map(|(edge, _)| edge.edge).collect::<Vec<_>>();
                let mut roots = store.projected_members(
                    false,
                    label,
                    &ids,
                    predicate_projection(&step.edge).flatten(),
                )?;
                let missing: Vec<_> = edges
                    .iter()
                    .map(|(edge, _)| *edge)
                    .filter(|e| !roots.contains_key(&e.edge))
                    .collect();
                if !missing.is_empty() {
                    roots.extend(store.labeled_edge_roots(missing.into_iter())?);
                }
                if let Some(field) = predicate_projection(&step.edge).filter(|_| roots.len() >= 16)
                {
                    store
                        .preload_projection(roots.values().map(|edge| edge.row).collect(), field)?;
                }
                let mut endpoints = Vec::with_capacity(end);
                for &(edge, row) in edges {
                    let entry = roots[&edge.edge];
                    if let Some(bound) = match_element_with_proof(
                        store,
                        atoms,
                        row,
                        &step.edge,
                        entry,
                        ElementProof {
                            label: Some(0),
                            property: None,
                        },
                    )? {
                        endpoints.push((edge.node, bound));
                    }
                }
                endpoints
            } else {
                edges
                    .iter()
                    .map(|(edge, row)| (edge.node, Cow::Borrowed(*row)))
                    .collect()
            };
            match_fixed_targets(store, atoms, &step.node, node_field, endpoints, &mut out)?;
        }
    }
    if node_field.is_some() {
        store.clear_projection();
    }
    Ok(out)
}

fn match_fixed_targets<'p>(
    store: &mut GraphStore<'_>,
    atoms: &[Expr],
    node: &'p Element,
    field: Option<Option<&str>>,
    mut endpoints: Vec<(u64, Cow<'_, Bindings<'p>>)>,
    out: &mut Vec<(u64, Bindings<'p>)>,
) -> Result<(), QueryError> {
    let initial_len = out.len();
    let label = node.labels.first();
    if let Some(label) = label {
        let ids: Vec<_> = endpoints.iter().map(|(id, _)| *id).collect();
        let projected = store.projected_members(true, label, &ids, field.flatten())?;
        let unknown = ids
            .into_iter()
            .filter(|id| !projected.contains_key(id))
            .collect();
        let members = store.label_members(true, label, unknown)?;
        endpoints.retain(|(id, _)| projected.contains_key(id) || members.contains(id));
    }
    let check_node = node.variable.is_some()
        || node.labels.len() > 1
        || !node.properties.is_empty()
        || node.predicate.is_some();
    if check_node && endpoints.len() >= 16 {
        store.preload_reached_nodes(endpoints.iter().map(|(id, _)| *id))?;
    }
    if let Some(field) = field.filter(|_| endpoints.len() >= 16) {
        let mut row_ids = Vec::with_capacity(endpoints.len());
        for &(id, _) in &endpoints {
            if let Some(node) = store.lookup_reached_node(id)? {
                row_ids.push(node.row);
            }
        }
        store.preload_projection(row_ids, field)?;
    }
    let proof = ElementProof {
        label: label.map(|_| 0),
        property: None,
    };
    for (id, row) in endpoints {
        let row = if check_node {
            let Some(entry) = store.lookup_reached_node(id)? else {
                continue;
            };
            let Some(matched) = match_element_with_proof(store, atoms, &row, node, entry, proof)?
            else {
                continue;
            };
            match matched {
                Cow::Borrowed(_) => row.into_owned(),
                Cow::Owned(row) => row,
            }
        } else {
            row.into_owned()
        };
        reserve_binding(store, &row)?;
        out.push((id, row));
    }
    store.record_survivors(out.len() - initial_len);
    Ok(())
}

fn optional_bind<'p>(row: &mut Bindings<'p>, variable: Option<&'p String>) {
    if let Some(name) = variable.filter(|name| !row.contains_key(name)) {
        row.insert(name, Bound::Null);
    }
}

#[derive(Clone)]
struct Walk<'p> {
    row: Bindings<'p>,
    node: u64,
    nodes: SmallVec<[u64; 5]>,
    edges: SmallVec<[ElementRef; 4]>,
}

fn match_pattern<'p>(
    store: &mut GraphStore<'_>,
    atoms: &[Expr],
    row: Bindings<'p>,
    pattern: &'p Pattern,
    predicate: Option<&Expression>,
) -> Result<Vec<Bindings<'p>>, QueryError> {
    let (starts, proof) = start_nodes(store, atoms, &row, &pattern.start, predicate)?;
    let mut out = Vec::new();
    for start in starts {
        let Some(row) = match_element_with_proof(store, atoms, &row, &pattern.start, start, proof)?
        else {
            continue;
        };
        if pattern.mode == PathMode::Walk
            && !pattern.shortest
            && pattern.path.is_none()
            && !pattern.steps.is_empty()
            && pattern
                .steps
                .iter()
                .all(|step| step.edge.variable.is_none())
        {
            let mut endpoints = vec![(start.id, row.into_owned())];
            for step in &pattern.steps {
                endpoints = expand_vertices(store, atoms, endpoints, step)?;
                if endpoints.is_empty() {
                    break;
                }
            }
            for (_, row) in endpoints {
                if passes(store, atoms, &row, predicate)? {
                    out.push(row);
                }
            }
            continue;
        }
        let mut frontier = vec![Walk {
            row: row.into_owned(),
            node: start.id,
            nodes: smallvec![start.id],
            edges: SmallVec::new(),
        }];
        for step in &pattern.steps {
            let simple_shortest = pattern.shortest
                && pattern.steps.len() == 1
                && pattern.mode == PathMode::Walk
                && step.quantified;
            frontier = expand(store, atoms, frontier, step, pattern.mode, simple_shortest)?;
            if frontier.is_empty() {
                break;
            }
        }
        if pattern.shortest {
            frontier.sort_by_key(|walk| walk.edges.len());
        }
        let mut selected = HashSet::new();
        for mut walk in frontier {
            if pattern.shortest && selected.contains(&walk.node) {
                continue;
            }
            if let Some(name) = &pattern.path {
                let path = Bound::Path(Path {
                    nodes: walk.nodes.into_vec(),
                    edges: walk.edges.into_vec(),
                });
                if !bind(&mut walk.row, name, path) {
                    continue;
                }
            }
            if pattern.shortest {
                selected.insert(walk.node);
            }
            if passes(store, atoms, &walk.row, predicate)? {
                reserve_binding(store, &walk.row)?;
                out.push(walk.row);
            }
        }
    }
    Ok(out)
}

#[derive(Clone, Copy, Default)]
struct ElementProof {
    label: Option<usize>,
    property: Option<usize>,
}

fn start_nodes(
    store: &mut GraphStore<'_>,
    atoms: &[Expr],
    row: &Bindings<'_>,
    element: &Element,
    predicate: Option<&Expression>,
) -> Result<(Vec<ElementRef>, ElementProof), QueryError> {
    let unconstrained = ElementProof::default();
    if let Some(bound) = element.variable.as_ref().and_then(|name| row.get(name)) {
        return match bound {
            Bound::Element(entry) if entry.node() => Ok((vec![*entry], unconstrained)),
            Bound::Null => Ok((Vec::new(), unconstrained)),
            _ => Err(invalid("Node pattern variable must bind a node")),
        };
    }
    let constraint = predicate
        .and_then(|expr| find_constraint(expr, element.variable.as_deref(), row))
        .or_else(|| {
            element
                .predicate
                .as_ref()
                .and_then(|expr| find_constraint(expr, element.variable.as_deref(), row))
        });
    if let Some((key, expression)) = constraint {
        let value = eval(store, atoms, row, expression)?;
        if let Some(key) = key {
            let (nodes, label, _) = store.node_candidates(&element.labels, Some((key, &value)))?;
            return Ok((
                nodes,
                ElementProof {
                    label,
                    property: None,
                },
            ));
        }
        return if let Some(id) = value.as_str().and_then(|value| value.parse().ok()) {
            Ok((store.lookup(id)?.into_iter().collect(), unconstrained))
        } else {
            Ok((Vec::new(), unconstrained))
        };
    }
    if let Some((slot, (key, expression))) = element
        .properties
        .iter()
        .enumerate()
        .find(|(_, (_, expression))| ready(expression, row))
    {
        let value = eval(store, atoms, row, expression)?;
        let (nodes, label, exact) = store.node_candidates(&element.labels, Some((key, &value)))?;
        return Ok((
            nodes,
            ElementProof {
                label,
                property: exact.then_some(slot),
            },
        ));
    }
    let (nodes, label, _) = store.node_candidates(&element.labels, None)?;
    Ok((
        nodes,
        ElementProof {
            label,
            property: None,
        },
    ))
}

fn find_constraint<'a>(
    expr: &'a Expression,
    variable: Option<&str>,
    row: &Bindings<'_>,
) -> Option<(Option<&'a str>, &'a Expression)> {
    let variable = variable?;
    match expr {
        Expression::Binary(a, Operator::And, b) => find_constraint(a, Some(variable), row)
            .or_else(|| find_constraint(b, Some(variable), row)),
        Expression::Binary(a, Operator::Eq, b) => {
            let key = |expr: &'a Expression| match expr {
                Expression::Property(name, key) if name == variable => Some(Some(key.as_str())),
                Expression::ElementId(name) if name == variable => Some(None),
                _ => None,
            };
            if let Some(key) = key(a).filter(|_| ready(b, row)) {
                Some((key, b))
            } else {
                key(b)
                    .filter(|_| ready(a, row))
                    .map(|key| (key, a.as_ref()))
            }
        }
        _ => None,
    }
}

fn ready(expr: &Expression, row: &Bindings<'_>) -> bool {
    match expr {
        Expression::Count(_) => false,
        Expression::Atom(_) => true,
        Expression::Record(fields) => fields.iter().all(|(_, expr)| ready(expr, row)),
        Expression::Variable(name)
        | Expression::Property(name, _)
        | Expression::ElementId(name)
        | Expression::PathLength(name) => row.contains_key(name),
        Expression::Cast(expr, _) | Expression::Not(expr) | Expression::IsNull(expr, _) => {
            ready(expr, row)
        }
        Expression::Binary(a, _, b) => ready(a, row) && ready(b, row),
    }
}

fn expand_vertices<'p>(
    store: &mut GraphStore<'_>,
    atoms: &[Expr],
    starts: Vec<(u64, Bindings<'p>)>,
    step: &'p Step,
) -> Result<Vec<(u64, Bindings<'p>)>, QueryError> {
    let mut output = Vec::new();
    let match_edges = !step.edge.labels.is_empty()
        || !step.edge.properties.is_empty()
        || step.edge.predicate.is_some();
    let match_nodes = step.node.variable.is_some()
        || !step.node.labels.is_empty()
        || !step.node.properties.is_empty()
        || step.node.predicate.is_some();
    for (start, row) in starts {
        let mut frontier = vec![start];
        for depth in 0..=step.upper {
            if depth >= step.lower {
                if match_nodes && frontier.len() >= 16 {
                    store.preload_reached_nodes(frontier.iter().copied())?;
                }
                for &id in &frontier {
                    let matched = if match_nodes {
                        match store.lookup_reached_node(id)? {
                            Some(node) => match_element(store, atoms, &row, &step.node, node)?,
                            None => None,
                        }
                    } else {
                        Some(Cow::Borrowed(&row))
                    };
                    if let Some(matched) = matched {
                        reserve_binding(store, &matched)?;
                        output.push((id, matched.into_owned()));
                    }
                }
            }
            if depth == step.upper {
                break;
            }
            let mut next = Vec::new();
            let batch_size = adjacency_batch_size(store.txn);
            for window in frontier.chunks(ADJACENCY_WINDOW_SIZE) {
                let prefetched = store.adjacent_window(
                    window,
                    step.direction,
                    !match_edges,
                    batch_size,
                    AdjacencyBounds::default(),
                )?;
                for (anchors, batch) in window.chunks(batch_size).zip(prefetched) {
                    if let Some(batch) = &batch {
                        store.account_adjacency_batch(batch)?;
                    }
                    for &id in anchors {
                        let single;
                        let neighbors = if let Some(batch) = &batch {
                            if !match_edges {
                                batch.append_vertices(id, &mut next);
                                continue;
                            }
                            batch.neighbors(id)
                        } else {
                            single = store.adjacent(id, step.direction)?;
                            single.as_slice()
                        };
                        for neighbor in neighbors {
                            if !match_edges
                                || match_element(store, atoms, &row, &step.edge, neighbor.edge)?
                                    .is_some()
                            {
                                next.push(neighbor.node);
                            }
                        }
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
    }
    Ok(output)
}

fn expand<'p>(
    store: &mut GraphStore<'_>,
    atoms: &[Expr],
    walks: Vec<Walk<'p>>,
    step: &'p Step,
    mode: PathMode,
    simple_shortest: bool,
) -> Result<Vec<Walk<'p>>, QueryError> {
    let mut output = Vec::new();
    for walk in walks {
        let prefix = walk.edges.len();
        let mut visited = HashSet::new();
        if simple_shortest {
            visited.insert((walk.node, 0));
        }
        let target = step
            .node
            .variable
            .as_ref()
            .and_then(|name| match walk.row.get(name) {
                Some(Bound::Element(entry)) => Some(entry.id),
                _ => None,
            });
        let mut frontier = vec![walk];
        let bind_endpoint = step.node.variable.is_some()
            || !step.node.labels.is_empty()
            || !step.node.properties.is_empty()
            || step.node.predicate.is_some();
        for depth in 0..=step.upper {
            let mut next = Vec::new();
            if bind_endpoint && depth >= step.lower && frontier.len() >= 16 {
                store.preload_reached_nodes(frontier.iter().map(|walk| walk.node))?;
            }
            for walk in frontier {
                if depth >= step.lower {
                    let matched = if bind_endpoint {
                        match store.lookup(walk.node)? {
                            Some(node) => match_element(store, atoms, &walk.row, &step.node, node)?,
                            None => None,
                        }
                    } else {
                        Some(Cow::Borrowed(&walk.row))
                    };
                    if let Some(mut row) = matched {
                        let group_matched = !step.quantified
                            || step.edge.variable.as_ref().is_none_or(|name| {
                                bind(
                                    row.to_mut(),
                                    name,
                                    Bound::Group(walk.edges[prefix..].to_vec()),
                                )
                            });
                        if group_matched {
                            reserve_binding(store, &row)?;
                            let finished = simple_shortest && target == Some(walk.node);
                            output.push(Walk {
                                row: row.into_owned(),
                                node: walk.node,
                                nodes: walk.nodes.clone(),
                                edges: walk.edges.clone(),
                            });
                            if finished {
                                return Ok(output);
                            }
                        }
                    }
                }
                if depth == step.upper {
                    continue;
                }
                for neighbor in store.adjacent(walk.node, step.direction)? {
                    if (mode == PathMode::Trail && walk.edges.contains(&neighbor.edge))
                        || (mode == PathMode::Acyclic && walk.nodes.contains(&neighbor.node))
                    {
                        continue;
                    }
                    let Some(edge_row) =
                        match_element(store, atoms, &walk.row, &step.edge, neighbor.edge)?
                    else {
                        continue;
                    };
                    if simple_shortest
                        && !visited.insert((neighbor.node, (depth + 1).min(step.lower)))
                    {
                        continue;
                    }
                    // Edges are visible in this view and both endpoints are
                    // protected by the same graph transaction. Fetch a node
                    // only when the ending pattern actually consumes it.
                    let node = neighbor.node;
                    reserve_binding(store, &walk.row)?;
                    let row = if step.quantified {
                        walk.row.clone()
                    } else {
                        edge_row.into_owned()
                    };
                    let mut candidate = Walk {
                        row,
                        node,
                        nodes: walk.nodes.clone(),
                        edges: walk.edges.clone(),
                    };
                    candidate.nodes.push(node);
                    candidate.edges.push(neighbor.edge);
                    next.push(candidate);
                }
            }
            frontier = next;
            if frontier.is_empty() {
                break;
            }
        }
    }
    Ok(output)
}

fn match_element<'a, 'p>(
    store: &GraphStore<'_>,
    atoms: &[Expr],
    row: &'a Bindings<'p>,
    pattern: &'p Element,
    entry: ElementRef,
) -> Result<Option<Cow<'a, Bindings<'p>>>, QueryError> {
    match_element_with_proof(store, atoms, row, pattern, entry, ElementProof::default())
}

fn match_element_with_proof<'a, 'p>(
    store: &GraphStore<'_>,
    atoms: &[Expr],
    row: &'a Bindings<'p>,
    pattern: &'p Element,
    entry: ElementRef,
    proof: ElementProof,
) -> Result<Option<Cow<'a, Bindings<'p>>>, QueryError> {
    let mut row = Cow::Borrowed(row);
    if let Some(name) = &pattern.variable {
        match row.get(name) {
            Some(Bound::Element(existing)) if existing == &entry => (),
            Some(_) => return Ok(None),
            None => {
                row.to_mut().insert(name, Bound::Element(entry));
            }
        }
    }
    for (slot, label) in pattern.labels.iter().enumerate() {
        if proof.label != Some(slot) && !store.has_label(entry, label)? {
            return Ok(None);
        }
    }
    if pattern.properties.len() > usize::from(proof.property.is_some()) {
        let props = store.properties(entry)?;
        for (slot, (key, expression)) in pattern.properties.iter().enumerate() {
            if proof.property == Some(slot) {
                continue;
            }
            let value = eval(store, atoms, &row, expression)?;
            if value.is_null()
                || !props.get(key).is_some_and(|property| {
                    values::compare(property, &value) == Some(Ordering::Equal)
                })
            {
                return Ok(None);
            }
        }
    }
    Ok(passes(store, atoms, &row, pattern.predicate.as_ref())?.then_some(row))
}

fn passes(
    store: &GraphStore<'_>,
    atoms: &[Expr],
    row: &Bindings<'_>,
    predicate: Option<&Expression>,
) -> Result<bool, QueryError> {
    predicate.map_or(Ok(true), |expression| {
        Ok(values::truth(&eval(store, atoms, row, expression)?)? == Some(true))
    })
}

fn bind<'p>(row: &mut Bindings<'p>, name: &'p str, value: Bound) -> bool {
    if let Some(old) = row.get(name) {
        return old == &value;
    }
    row.insert(name, value);
    true
}

fn check_scalar(value: Option<&Json>) -> Result<(), QueryError> {
    if value.is_some_and(|value| value.is_array() || value.is_object()) {
        return Err(invalid("GQL properties must be scalar values"));
    }
    Ok(())
}

fn properties(
    store: &GraphStore<'_>,
    atoms: &[Expr],
    row: &Bindings<'_>,
    pattern: &Element,
) -> Result<Map<String, Json>, QueryError> {
    let mut props = Map::new();
    for (key, expression) in &pattern.properties {
        let value = eval(store, atoms, row, expression)?;
        check_scalar(Some(&value))?;
        if props.insert(key.clone(), value).is_some() {
            return Err(invalid("Duplicate GQL property"));
        }
    }
    Ok(props)
}

fn bind_inserted<'p>(
    row: &mut Bindings<'p>,
    pattern: &'p Element,
    entry: ElementRef,
) -> Result<(), QueryError> {
    if let Some(variable) = &pattern.variable {
        if !bind(row, variable, Bound::Element(entry)) {
            return Err(invalid("INSERT cannot replace a bound GQL variable"));
        }
    }
    Ok(())
}

fn insert_node<'p>(
    store: &mut GraphStore<'_>,
    atoms: &[Expr],
    row: &mut Bindings<'p>,
    pattern: &'p Element,
) -> Result<(ElementRef, bool), QueryError> {
    if let Some(value) = pattern.variable.as_ref().and_then(|name| row.get(name)) {
        return match value {
            Bound::Element(entry)
                if entry.node() && pattern.labels.is_empty() && pattern.properties.is_empty() =>
            {
                Ok((*entry, false))
            }
            _ => Err(invalid(
                "A bound INSERT endpoint requires a node variable without labels or properties",
            )),
        };
    }
    let props = properties(store, atoms, row, pattern)?;
    let node = store.insert_node(&pattern.labels, props)?;
    bind_inserted(row, pattern, node)?;
    Ok((node, true))
}
