//! Share exact grouped values and a filtered grouped threshold in one producer.
use super::*;
use crate::parser::{visit, GroupByModifier, Join, JoinType};

pub(super) fn ordinary(s: &Select) -> bool {
    s.values.is_none()
        && s.json_table.is_none()
        && s.joins.iter().all(|j| {
            j.join_type == JoinType::Cross
                && j.condition.is_none()
                && j.derived.is_none()
                && !j.lateral
                && !j.straight
                && !j.natural
                && j.using_columns.is_empty()
        })
        && !s.distinct
        && s.grouping_sets.is_empty()
        && s.group_by_modifier == GroupByModifier::None
        && s.order_by.is_empty()
        && s.limit.is_none()
        && s.offset.is_none()
        && !s.for_update
        && s.lock_clause.is_none()
        && !s.sql_calc_found_rows
        && !s.straight_join
        && s.query_hints.is_empty()
        && s.named_windows.is_empty()
        && s.rls_security_context.is_none()
        && s.rls_expression_settings.is_none()
        && s.from
            .iter()
            .chain(s.joins.iter().map(|j| &j.table))
            .all(|t| {
                t.system_time.is_none()
                    && t.sample.is_none()
                    && t.index_hints.is_empty()
                    && t.partitions.is_empty()
                    && t.column_aliases.is_empty()
                    && t.generate_series.is_none()
            })
}
pub(super) fn sources(s: &Select) -> Vec<&TableRef> {
    s.from
        .iter()
        .chain(s.joins.iter().map(|j| &j.table))
        .collect()
}
fn raw(e: &Expr) -> &Expr {
    if let Expr::GroupKeyRef { expr, .. } = e {
        raw(expr)
    } else {
        e
    }
}
fn sum(e: &Expr) -> Option<&Expr> {
    match raw(e) {
        Expr::Function {
            name,
            args,
            distinct: false,
        } if name.eq_ignore_ascii_case("sum") && args.len() == 1 => Some(&args[0]),
        _ => None,
    }
}
pub(super) fn col<'a>(
    db: &Arc<Database>,
    database: &str,
    s: &'a Select,
    e: &Expr,
) -> Option<(usize, usize, Arc<crate::storage::TableSchema>)> {
    let (qualifier, name) = match raw(e) {
        Expr::Identifier(n) => (None, n),
        Expr::CompoundIdentifier(p) if p.len() == 2 => (Some(&p[0]), &p[1]),
        _ => return None,
    };
    let matches = s
        .from
        .iter()
        .chain(s.joins.iter().map(|j| &j.table))
        .enumerate()
        .filter_map(|(i, t)| {
            if qualifier.is_some_and(|q| {
                !q.eq_ignore_ascii_case(t.alias.as_ref().unwrap_or(&t.name))
                    && !q.eq_ignore_ascii_case(t.name.rsplit('.').next().unwrap_or(&t.name))
            }) {
                return None;
            }
            let (d, n) = crate::storage::split_table_name(database, &t.name);
            let schema = db.table_in_database(&d, &n).ok()?;
            let c = schema.column_index(name).ok()?;
            Some((i, c, schema))
        })
        .collect::<Vec<_>>();
    if matches.len() == 1 {
        matches.into_iter().next()
    } else {
        None
    }
}
pub(super) fn exact_value(db: &Arc<Database>, database: &str, s: &Select, e: &Expr) -> bool {
    match raw(e) {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            col(db, database, s, e).is_some_and(|(_, c, t)| {
                matches!(
                    t.columns[c].data_type,
                    crate::storage::DataTypeKind::TinyInt
                        | crate::storage::DataTypeKind::SmallInt
                        | crate::storage::DataTypeKind::MediumInt
                        | crate::storage::DataTypeKind::Int
                        | crate::storage::DataTypeKind::BigInt
                        | crate::storage::DataTypeKind::Decimal { .. }
                )
            })
        }
        Expr::Literal(crate::parser::Literal::Int(_) | crate::parser::Literal::UInt(_))
        | Expr::BoundValue(Value::Int(_) | Value::UInt(_)) => true,
        Expr::Binary {
            left,
            op: BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul,
            right,
        } => exact_value(db, database, s, left) && exact_value(db, database, s, right),
        _ => false,
    }
}
fn dim_filter(db: &Arc<Database>, database: &str, s: &Select, e: &Expr, dim: usize) -> bool {
    if crate::planner::expr_literal_value(e)
        .is_some_and(|v| matches!(v, Value::Int(_) | Value::UInt(_) | Value::Null))
    {
        return true;
    }
    match raw(e) {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            col(db, database, s, e).is_some_and(|(t, c, s)| {
                t == dim
                    && matches!(
                        s.columns[c].data_type,
                        crate::storage::DataTypeKind::TinyInt
                            | crate::storage::DataTypeKind::SmallInt
                            | crate::storage::DataTypeKind::MediumInt
                            | crate::storage::DataTypeKind::Int
                            | crate::storage::DataTypeKind::BigInt
                    )
            })
        }
        Expr::Binary {
            left,
            op:
                BinaryOp::And
                | BinaryOp::Or
                | BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq,
            right,
        } => dim_filter(db, database, s, left, dim) && dim_filter(db, database, s, right, dim),
        Expr::InList { expr, list, .. } => {
            dim_filter(db, database, s, expr, dim)
                && list.iter().all(|v| dim_filter(db, database, s, v, dim))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            dim_filter(db, database, s, expr, dim)
                && dim_filter(db, database, s, low, dim)
                && dim_filter(db, database, s, high, dim)
        }
        Expr::IsNull(e) | Expr::IsNotNull(e) => dim_filter(db, database, s, e, dim),
        _ => false,
    }
}
fn conjuncts(e: Option<&Expr>) -> Vec<Expr> {
    let mut out = Vec::new();
    if let Some(e) = e {
        split_and(e, &mut out);
    }
    out
}
fn and_all(mut exprs: Vec<Expr>) -> Option<Expr> {
    let first = exprs.pop()?;
    Some(exprs.into_iter().fold(first, |right, left| Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::And,
        right: Box::new(right),
    }))
}
fn rewrite_expr(e: &Expr, groups: &[Expr], aggregate: &Expr) -> Expr {
    let mut result = e.clone();
    visit::rewrite_value_expr(&mut result, |e| {
        if raw(e) == raw(aggregate) {
            *e = Expr::Identifier("__shared_total".into());
            return false;
        }
        if let Some(i) = groups.iter().position(|g| raw(g) == raw(e)) {
            *e = Expr::Identifier(format!("__shared_key_{i}"));
            return false;
        }
        true
    });
    result
}
fn candidate(
    db: &Arc<Database>,
    database: &str,
    ctes: &[CteDef],
    max_index: usize,
    best_index: usize,
) -> Option<(Select, Select, Select)> {
    let outer = ctes[max_index].query.as_select()?;
    let best = ctes[best_index].query.as_select()?;
    if !ctes[max_index].columns.is_empty()
        || !ctes[best_index].columns.is_empty()
        || ctes[max_index].is_recursive
        || ctes[best_index].is_recursive
        || !ordinary(outer)
        || !ordinary(best)
        || best.derived.is_some()
        || best.group_by.is_empty()
        || best.group_by.len() > 4
        || best.having.is_none()
        || outer.selection.is_some()
        || outer.having.is_some()
        || !outer.group_by.is_empty()
        || sources(outer).len() != 1
    {
        return None;
    }
    let inner = outer.derived.as_ref()?.as_select()?;
    if !ordinary(inner)
        || inner.derived.is_some()
        || inner.having.is_some()
        || inner.group_by != best.group_by
        || sources(inner).len() != sources(best).len() + 1
    {
        return None;
    }
    let [SelectItem::Expr {
        expr:
            Expr::Function {
                name: outer_fn,
                args: outer_args,
                distinct: false,
            },
        ..
    }] = outer.projection.as_slice()
    else {
        return None;
    };
    if !["max", "min", "sum", "avg"]
        .iter()
        .any(|n| outer_fn.eq_ignore_ascii_case(n))
        || outer_args.len() != 1
    {
        return None;
    }
    let summed = inner.projection.iter().find_map(|p| match p {
        SelectItem::Expr {
            expr,
            alias: Some(a),
        } if sum(expr).is_some()
            && matches!(&outer_args[0],Expr::Identifier(n) if n.eq_ignore_ascii_case(a)) =>
        {
            Some(expr)
        }
        _ => None,
    })?;
    if inner.projection.iter().any(|p| {
        !matches!(p,SelectItem::Expr{expr,..}
        if raw(expr)==raw(summed) || inner.group_by.iter().any(|g| raw(g)==raw(expr)))
    }) {
        return None;
    }
    if sources(inner)
        .iter()
        .any(|t| has_cte(&t.name) || ctes.iter().any(|c| c.name.eq_ignore_ascii_case(&t.name)))
    {
        return None;
    }
    let argument = sum(summed)?;
    if !exact_value(db, database, best, argument)
        || best
            .group_by
            .iter()
            .any(|g| col(db, database, best, g).is_none())
    {
        return None;
    }
    let best_sum = best
        .projection
        .iter()
        .filter_map(|p| match p {
            SelectItem::Expr { expr, .. } if sum(expr) == Some(argument) => Some(expr),
            _ => None,
        })
        .next()?;
    if best.projection.iter().any(|p|!matches!(p,SelectItem::Expr{expr,..} if sum(expr)==Some(argument)||best.group_by.iter().any(|g|raw(g)==raw(expr)))) {return None;}
    let max_name = &ctes[max_index].name;
    let mut uses_max = false;
    let having_safe = visit::try_inspect_value_expr(best.having.as_ref()?, |e| {
        if raw(e) == raw(best_sum) || best.group_by.iter().any(|g| raw(g) == raw(e)) {
            return Ok(false);
        }
        match e {
            Expr::Subquery(q)
                if q.from.len() == 1
                    && q.from[0].name.eq_ignore_ascii_case(max_name)
                    && ordinary(q)
                    && q.derived.is_none()
                    && q.selection.is_none()
                    && q.group_by.is_empty()
                    && q.having.is_none()
                    && matches!(
                        q.projection.as_slice(),
                        [SelectItem::Wildcard]
                            | [SelectItem::Expr {
                                expr: Expr::Identifier(_),
                                ..
                            }]
                    ) =>
            {
                uses_max = true;
                return Ok(false);
            }
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::GroupKeyRef { .. }
            | Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
            | Expr::WindowFunction { .. } => return Err(()),
            Expr::Function { name, .. } if crate::parser::is_aggregate_function(name) => {
                return Err(())
            }
            _ => {}
        }
        Ok(true)
    })
    .is_ok();
    if !having_safe {
        return None;
    }
    if !uses_max {
        return None;
    }
    if select_has_nondeterministic(inner) || select_has_nondeterministic(best) {
        return None;
    }
    let all = sources(inner);
    let base = sources(best);
    let extra = all
        .iter()
        .enumerate()
        .filter(|(_, t)| !base.contains(t))
        .map(|(i, t)| (i, *t))
        .collect::<Vec<_>>();
    if extra.len() != 1 || base.iter().any(|t| !all.contains(t)) {
        return None;
    }
    let (dim, dimension) = extra[0];
    let mut extra_predicates = conjuncts(inner.selection.as_ref());
    for common in conjuncts(best.selection.as_ref()) {
        let i = extra_predicates.iter().position(|p| p == &common)?;
        extra_predicates.remove(i);
    }
    let mut join = None;
    let mut filters = Vec::new();
    for p in extra_predicates {
        if let Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } = &p
        {
            for (dk, fk) in [(left, right), (right, left)] {
                if let (Some((d, dc, ds)), Some((f, fc, fs))) =
                    (col(db, database, inner, dk), col(db, database, inner, fk))
                {
                    if d == dim
                        && f != dim
                        && !ds.columns[dc].nullable
                        && matches!(
                            ds.columns[dc].data_type,
                            crate::storage::DataTypeKind::TinyInt
                                | crate::storage::DataTypeKind::SmallInt
                                | crate::storage::DataTypeKind::MediumInt
                                | crate::storage::DataTypeKind::Int
                                | crate::storage::DataTypeKind::BigInt
                        )
                        && ds.columns[dc].data_type == fs.columns[fc].data_type
                        && ds.columns[dc].unsigned == fs.columns[fc].unsigned
                        && ds.iter_queryable_indexes().any(|i| {
                            i.unique
                                && i.columns.len() == 1
                                && !i.has_prefix_columns()
                                && !i.has_expression_parts()
                                && i.columns[0].eq_ignore_ascii_case(&ds.columns[dc].name)
                        })
                    {
                        if join.is_some() {
                            return None;
                        }
                        join = Some((p.clone(), dk.as_ref().clone()));
                    }
                }
            }
            if join.as_ref().is_some_and(|(j, _)| j == &p) {
                continue;
            }
        }
        if !dim_filter(db, database, inner, &p, dim) {
            return None;
        }
        filters.push(p);
    }
    let (join_condition, presence) = join?;
    let mut producer = best.clone();
    producer.having = None;
    producer.projection = best
        .group_by
        .iter()
        .enumerate()
        .map(|(i, g)| SelectItem::Expr {
            expr: raw(g).clone(),
            alias: Some(format!("__shared_key_{i}")),
        })
        .collect();
    producer.projection.push(SelectItem::Expr {
        expr: best_sum.clone(),
        alias: Some("__shared_total".into()),
    });
    producer.projection.push(SelectItem::Expr {
        expr: Expr::Function {
            name: "SUM".into(),
            args: vec![Expr::Case {
                operand: None,
                conditions: vec![Expr::IsNotNull(Box::new(presence))].into(),
                results: vec![argument.clone()].into(),
                else_result: None,
            }],
            distinct: false,
        },
        alias: Some("__shared_filtered".into()),
    });
    filters.push(join_condition);
    producer.joins.push(Join {
        table: dimension.clone(),
        join_type: JoinType::Left,
        straight: false,
        condition: and_all(filters),
        using_columns: Vec::new(),
        natural: false,
        derived: None,
        lateral: false,
    });
    let mut maximum = outer.clone();
    maximum.derived = None;
    maximum.from.clear();
    if let SelectItem::Expr {
        expr: Expr::Function { args, .. },
        ..
    } = &mut maximum.projection[0]
    {
        args[0] = Expr::Identifier("__shared_filtered".into());
    }
    let mut consumer = best.clone();
    consumer.selection = Some(rewrite_expr(
        best.having.as_ref()?,
        &best.group_by,
        best_sum,
    ));
    consumer.having = None;
    consumer.group_by.clear();
    consumer.joins.clear();
    for p in &mut consumer.projection {
        if let SelectItem::Expr { expr, alias } = p {
            let name = alias.clone().or_else(|| expr_output_name(raw(expr)))?;
            *expr = rewrite_expr(expr, &best.group_by, best_sum);
            *alias = Some(name);
        }
    }
    Some((producer, maximum, consumer))
}
pub(super) fn share(db: &Arc<Database>, database: &str, ctes: &mut Vec<CteDef>) {
    if !crate::util::env::flag("MYDB_GROUPED_THRESHOLD_SHARE", true)
        && !(cfg!(test)
            && crate::util::env::var("MYDB_GROUPED_THRESHOLD_SHARE").is_ok_and(|v| v == "force"))
    {
        return;
    }
    for m in 0..ctes.len() {
        for b in 0..ctes.len() {
            if m == b {
                continue;
            }
            let Some((producer, mut maximum, mut consumer)) = candidate(db, database, ctes, m, b)
            else {
                continue;
            };
            let forced = cfg!(test)
                && crate::util::env::var("MYDB_GROUPED_THRESHOLD_SHARE")
                    .is_ok_and(|v| v == "force");
            if !forced && cte_body_scan_volume(db, database, &producer) < 100_000 {
                continue;
            }
            let name = (0..)
                .map(|i| format!("__shared_group_{i}"))
                .find(|n| {
                    ctes.iter().all(|c| !c.name.eq_ignore_ascii_case(n))
                        && sources(&producer).iter().all(|t| {
                            !t.name
                                .rsplit('.')
                                .next()
                                .unwrap_or(&t.name)
                                .eq_ignore_ascii_case(n)
                        })
                })
                .unwrap();
            for s in [&mut maximum, &mut consumer] {
                s.from = vec![placeholder_table_ref(name.clone())];
                s.table = Some(name.clone());
            }
            ctes[m].query = maximum.into();
            ctes[b].query = consumer.into();
            ctes.insert(
                0,
                CteDef {
                    name,
                    columns: Vec::new(),
                    is_recursive: false,
                    query: producer.into(),
                },
            );
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{api::QueryResponse, StorageQueryEngine};
    #[test]
    fn grouped_threshold_share_preserves_global_domain_nulls_and_exact_sums() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        for sql in ["CREATE TABLE owners(k INT PRIMARY KEY)","CREATE TABLE periods(id INT PRIMARY KEY,keep_row INT)","CREATE TABLE events(k INT,p INT,v DECIMAL(30,6),qty INT)","INSERT INTO owners VALUES(1),(2),(3),(4)","INSERT INTO periods VALUES(10,1),(20,0)","INSERT INTO events VALUES(1,10,2,1),(1,20,20,1),(2,10,10,1),(2,20,-20,1),(3,20,30,1),(3,NULL,5,1),(3,999,7,1),(4,10,NULL,1),(99,10,1000,1)"] {engine.execute(sql).unwrap();}
        for filter in ["keep_row=1", "keep_row=2", "keep_row IN (0,1)"] {
            let sql=format!("WITH mx AS (SELECT MAX(part) threshold_value FROM (SELECT owners.k,SUM(v*qty) part FROM events,owners,periods WHERE events.k=owners.k AND p=id AND {filter} GROUP BY owners.k) t), chosen AS (SELECT owners.k,SUM(v*qty) total FROM events,owners WHERE events.k=owners.k GROUP BY owners.k HAVING SUM(v*qty)>0.95*(SELECT threshold_value FROM mx)) SELECT k,total FROM chosen ORDER BY k");
            let run = |gate| {
                crate::util::env::with_vars(&[("MYDB_GROUPED_THRESHOLD_SHARE", Some(gate))], || {
                    let QueryResponse::ResultSet(rs) = engine.execute(&sql).unwrap() else {
                        panic!()
                    };
                    rs
                })
            };
            let before = run("off");
            let after = run("force");
            assert_eq!(after.rows, before.rows, "{sql}");
            assert_eq!(
                after
                    .columns
                    .iter()
                    .map(|c| c.column_type)
                    .collect::<Vec<_>>(),
                before
                    .columns
                    .iter()
                    .map(|c| c.column_type)
                    .collect::<Vec<_>>()
            );
            crate::util::env::with_vars(&[("MYDB_GROUPED_THRESHOLD_SHARE", Some("force"))], || {
                let Statement::WithCte(mut with) = crate::parser::parse_statement(&sql).unwrap()
                else {
                    panic!()
                };
                share(engine.db_for_tests(), "test", &mut with.ctes);
                assert_eq!(with.ctes.len(), 3, "rule missed");
                assert!(with.ctes[0].name.starts_with("__shared_group_"));
            });
        }
    }
    #[test]
    fn grouped_threshold_share_rejects_unproved_multiplicity_and_observable_counts() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        for sql in [
            "CREATE TABLE owners(k INT PRIMARY KEY)",
            "CREATE TABLE periods(id INT,keep_row INT)",
            "CREATE TABLE events(k INT,p INT,v DECIMAL(20,4))",
        ] {
            engine.execute(sql).unwrap();
        }
        for extra in ["", " AND COUNT(*)>2"] {
            let sql=format!("WITH mx AS (SELECT MAX(part) threshold_value FROM (SELECT owners.k,SUM(v) part FROM events,owners,periods WHERE events.k=owners.k AND p=id AND keep_row=1 GROUP BY owners.k) t), chosen AS (SELECT owners.k,SUM(v) total FROM events,owners WHERE events.k=owners.k GROUP BY owners.k HAVING SUM(v)>(SELECT threshold_value FROM mx){extra}) SELECT * FROM chosen");
            let Statement::WithCte(mut with) = crate::parser::parse_statement(&sql).unwrap() else {
                panic!()
            };
            crate::util::env::with_vars(&[("MYDB_GROUPED_THRESHOLD_SHARE", Some("force"))], || {
                share(engine.db_for_tests(), "test", &mut with.ctes)
            });
            assert_eq!(with.ctes.len(), 2);
        }
    }
}
