//! Share a raw-input scalar AVG and duplicate-insensitive existence consumers.
//! ROLLUP retains the canonical AVG accumulator, including its global state.
use super::grouped_share::{col, exact_value, ordinary, sources};
use super::*;
use crate::parser::{GroupByModifier, Join, JoinType, QueryExpr};

fn leaves<'a>(query: &'a QueryExpr, union: bool, out: &mut Vec<&'a Select>) -> Option<()> {
    let mut pending = vec![query];
    while let Some(query) = pending.pop() {
        if pending.len() + out.len() > 32 {
            return None;
        }
        match query {
            QueryExpr::Select(s) => out.push(s),
            QueryExpr::SetOp(set)
                if set.order_by.is_empty()
                    && set.limit.is_none()
                    && set.offset.is_none()
                    && (if union {
                        set.op == SetOp::Union && set.all
                    } else {
                        !set.all
                    }) =>
            {
                pending.push(&set.right);
                pending.push(&set.left);
            }
            _ => return None,
        }
    }
    Some(())
}

fn canonical(db: &Arc<Database>, database: &str, s: &Select, e: &Expr) -> Option<Expr> {
    if let Some(value) = crate::planner::expr_literal_value(e) {
        return Some(Expr::BoundValue(value));
    }
    Some(match e {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let (_, c, t) = col(db, database, s, e)?;
            if t.row_security.enabled {
                return None;
            }
            Expr::Identifier(format!("__table_{}_column_{c}", t.id))
        }
        Expr::Literal(_) | Expr::BoundValue(_) => e.clone(),
        Expr::Binary { left, op, right } => {
            let mut left = canonical(db, database, s, left)?;
            let mut right = canonical(db, database, s, right)?;
            if *op == BinaryOp::Eq && format!("{left:?}") > format!("{right:?}") {
                std::mem::swap(&mut left, &mut right);
            }
            Expr::Binary {
                left: Box::new(left),
                op: *op,
                right: Box::new(right),
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(canonical(db, database, s, expr)?),
            low: Box::new(canonical(db, database, s, low)?),
            high: Box::new(canonical(db, database, s, high)?),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(canonical(db, database, s, expr)?),
            list: list
                .iter()
                .map(|e| canonical(db, database, s, e))
                .collect::<Option<_>>()?,
            negated: *negated,
        },
        Expr::IsNull(e) => Expr::IsNull(Box::new(canonical(db, database, s, e)?)),
        Expr::IsNotNull(e) => Expr::IsNotNull(Box::new(canonical(db, database, s, e)?)),
        _ => return None,
    })
}
fn predicates(db: &Arc<Database>, database: &str, s: &Select) -> Option<Vec<Expr>> {
    let mut terms = Vec::new();
    if let Some(e) = &s.selection {
        split_and(e, &mut terms);
    }
    let mut terms = terms
        .iter()
        .map(|e| canonical(db, database, s, e))
        .collect::<Option<Vec<_>>>()?;
    terms.sort_by_key(|e| format!("{e:?}"));
    Some(terms)
}
fn plain(s: &Select) -> bool {
    ordinary(s) && s.derived.is_none() && s.group_by.is_empty() && s.having.is_none()
}
fn projected(expr: Expr, alias: &str) -> SelectItem {
    SelectItem::Expr {
        expr,
        alias: Some(alias.into()),
    }
}
fn qualifier(table: &TableRef) -> String {
    table.alias.clone().unwrap_or_else(|| {
        table
            .name
            .rsplit('.')
            .next()
            .unwrap_or(&table.name)
            .to_string()
    })
}
fn id(name: &str) -> Expr {
    Expr::Identifier(name.into())
}
fn eq(a: Expr, b: Expr) -> Expr {
    Expr::Binary {
        left: Box::new(a),
        op: BinaryOp::Eq,
        right: Box::new(b),
    }
}
fn integer(n: usize) -> Expr {
    Expr::BoundValue(Value::Int(n as i64))
}
fn conjunction(terms: Vec<Expr>) -> Option<Expr> {
    terms.into_iter().reduce(|left, right| Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::And,
        right: Box::new(right),
    })
}

fn exact_argument(expr: &Expr, names: &HashSet<String>, qualifier: &str) -> bool {
    if let Some(value) = crate::planner::expr_literal_value(expr) {
        return matches!(
            value,
            Value::Int(_)
                | Value::UInt(_)
                | Value::Decimal(_)
                | Value::DecimalWide(_)
                | Value::Null
        );
    }
    match expr {
        Expr::Identifier(n) => names.contains(&n.to_ascii_lowercase()),
        Expr::CompoundIdentifier(p) if p.len() == 2 => {
            p[0].eq_ignore_ascii_case(qualifier) && names.contains(&p[1].to_ascii_lowercase())
        }
        Expr::Binary {
            left,
            op: BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul,
            right,
        } => exact_argument(left, names, qualifier) && exact_argument(right, names, qualifier),
        _ => false,
    }
}

struct Branch {
    select: Select,
    fact: usize,
    fact_id: u64,
}
fn branch(db: &Arc<Database>, database: &str, s: &Select, ctes: &[CteDef]) -> Option<Branch> {
    if !plain(s)
        || sources(s).len() != 2
        || s.projection.is_empty()
        || s.projection.len() > 8
        || select_has_nondeterministic(s)
    {
        return None;
    }
    let mut fact = None;
    for p in &s.projection {
        let SelectItem::Expr { expr, .. } = p else {
            return None;
        };
        let (t, _, schema) = col(db, database, s, expr)?;
        if !exact_value(db, database, s, expr) || fact.is_some_and(|(old, _)| old != t) {
            return None;
        }
        fact = Some((t, schema.id));
    }
    if sources(s)
        .iter()
        .any(|t| has_cte(&t.name) || ctes.iter().any(|c| c.name.eq_ignore_ascii_case(&t.name)))
    {
        return None;
    }
    predicates(db, database, s)?;
    let ids = sources(s)
        .iter()
        .map(|t| {
            let (d, n) = crate::storage::split_table_name(database, &t.name);
            db.table_in_database(&d, &n).map(|t| t.id).ok()
        })
        .collect::<Option<HashSet<_>>>()?;
    if ids.len() != 2 {
        return None;
    }
    let (fact, fact_id) = fact?;
    Some(Branch {
        select: s.clone(),
        fact,
        fact_id,
    })
}

/// Match a set leaf's extra equality against an AVG input. Only the owner
/// columns are observable through the enclosing DISTINCT set expression.
fn match_leaf(
    db: &Arc<Database>,
    database: &str,
    s: &Select,
    b: &Branch,
) -> Option<(Expr, TableRef, Expr)> {
    if !plain(s)
        || sources(s).len() != 3
        || s.projection.is_empty()
        || select_has_nondeterministic(s)
    {
        return None;
    }
    let mut owner = None;
    for p in &s.projection {
        let SelectItem::Expr { expr, .. } = p else {
            return None;
        };
        let (t, _, _) = col(db, database, s, expr)?;
        if owner.is_some_and(|old| old != t) {
            return None;
        }
        owner = Some(t);
    }
    let owner = owner?;
    let mut terms = Vec::new();
    if let Some(e) = &s.selection {
        split_and(e, &mut terms);
    }
    let common = predicates(db, database, &b.select)?;
    for (i, term) in terms.iter().enumerate() {
        let Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } = term
        else {
            continue;
        };
        for (fk, ok) in [(left, right), (right, left)] {
            let (Some((ft, fc, fs)), Some((ot, _, _))) =
                (col(db, database, s, fk), col(db, database, s, ok))
            else {
                continue;
            };
            if ft == owner || ot != owner || fs.id != b.fact_id {
                continue;
            }
            if !matches!(
                fs.columns[fc].data_type,
                crate::storage::DataTypeKind::TinyInt
                    | crate::storage::DataTypeKind::SmallInt
                    | crate::storage::DataTypeKind::MediumInt
                    | crate::storage::DataTypeKind::Int
                    | crate::storage::DataTypeKind::BigInt
            ) {
                continue;
            }
            let rest = terms
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, e)| canonical(db, database, s, e))
                .collect::<Option<Vec<_>>>()?;
            let mut normalized = rest;
            normalized.sort_by_key(|e| format!("{e:?}"));
            if normalized != common {
                continue;
            }
            // Resolve the fact key in the producer's own alias scope.
            let table = sources(&b.select)[b.fact];
            let key = Expr::CompoundIdentifier(vec![qualifier(table), fs.columns[fc].name.clone()]);
            let mut base_ids = sources(&b.select)
                .iter()
                .map(|t| {
                    let (d, n) = crate::storage::split_table_name(database, &t.name);
                    db.table_in_database(&d, &n).map(|t| t.id).ok()
                })
                .collect::<Option<Vec<_>>>()?;
            let mut input_ids = sources(s)
                .iter()
                .enumerate()
                .filter(|(t, _)| *t != owner)
                .map(|(_, t)| {
                    let (d, n) = crate::storage::split_table_name(database, &t.name);
                    db.table_in_database(&d, &n).map(|t| t.id).ok()
                })
                .collect::<Option<Vec<_>>>()?;
            base_ids.sort_unstable();
            input_ids.sort_unstable();
            if base_ids != input_ids {
                continue;
            }
            let (_, _, owner_schema) = col(db, database, s, ok)?;
            if base_ids.contains(&owner_schema.id) {
                return None;
            }
            return Some((key, sources(s)[owner].clone(), ok.as_ref().clone()));
        }
    }
    None
}

fn append_source(query: &mut QueryExpr, keys: &[Expr], channel: &str, key: &str, n: &mut usize) {
    match query {
        QueryExpr::Select(s) => {
            s.projection.push(projected(integer(*n), channel));
            s.projection.push(projected(keys[*n].clone(), key));
            *n += 1;
        }
        QueryExpr::SetOp(set) => {
            append_source(&mut set.left, keys, channel, key, n);
            append_source(&mut set.right, keys, channel, key, n);
        }
        _ => unreachable!("shared aggregate source was proved"),
    }
}
fn replace_leaves(query: &mut QueryExpr, replacements: &[Select], n: &mut usize) {
    match query {
        QueryExpr::Select(s) => {
            **s = replacements[*n].clone();
            *n += 1;
        }
        QueryExpr::SetOp(set) => {
            replace_leaves(&mut set.left, replacements, n);
            replace_leaves(&mut set.right, replacements, n);
        }
        _ => unreachable!("shared existence set was proved"),
    }
}

fn candidate(
    db: &Arc<Database>,
    database: &str,
    ctes: &[CteDef],
    a: usize,
    e: usize,
    name: &str,
) -> Option<(Select, Select, Select)> {
    let avg = ctes[a].query.as_select()?;
    let exists = ctes[e].query.as_select()?;
    if ctes[a].is_recursive
        || ctes[e].is_recursive
        || !ctes[a].columns.is_empty()
        || !ordinary(avg)
        || sources(avg).len() != 1
        || !avg.group_by.is_empty()
        || avg.having.is_some()
        || avg.selection.is_some()
    {
        return None;
    }
    let [SelectItem::Expr {
        expr:
            Expr::Function {
                name: fun,
                args,
                distinct: false,
            },
        ..
    }] = avg.projection.as_slice()
    else {
        return None;
    };
    if !fun.eq_ignore_ascii_case("avg") || args.len() != 1 || select_has_nondeterministic(avg) {
        return None;
    }
    let mut avg_leaves = Vec::new();
    leaves(avg.derived.as_ref()?, true, &mut avg_leaves)?;
    if avg_leaves.len() < 2 || avg_leaves.len() > 8 {
        return None;
    }
    let branches = avg_leaves
        .iter()
        .map(|s| branch(db, database, s, ctes))
        .collect::<Option<Vec<_>>>()?;
    let names = branches[0]
        .select
        .projection
        .iter()
        .map(|p| match p {
            SelectItem::Expr { expr, alias } => alias
                .clone()
                .or_else(|| expr_output_name(expr))
                .map(|n| n.to_ascii_lowercase()),
            _ => None,
        })
        .collect::<Option<HashSet<_>>>()?;
    let source = &avg.from[0];
    if names.len() != branches[0].select.projection.len()
        || !exact_argument(
            &args[0],
            &names,
            source.alias.as_ref().unwrap_or(&source.name),
        )
    {
        return None;
    }
    if branches
        .iter()
        .any(|b| b.select.projection.len() != branches[0].select.projection.len())
    {
        return None;
    }
    let (location, set) = if let Some(set) = exists.derived.as_ref() {
        (None, set)
    } else {
        let (i, join) = exists
            .joins
            .iter()
            .enumerate()
            .find(|(_, j)| matches!(j.derived, Some(QueryExpr::SetOp(_))))?;
        (Some(i), join.derived.as_ref()?)
    };
    if !matches!(set, QueryExpr::SetOp(_)) {
        return None;
    }
    let mut existence_leaves = Vec::new();
    leaves(set, false, &mut existence_leaves)?;
    let slots = (0..)
        .map(|n| format!("__shared_input_{n}"))
        .filter(|n| {
            branches[0].select.projection.iter().all(|p| match p {
                SelectItem::Expr {
                    alias: Some(alias), ..
                } => !alias.eq_ignore_ascii_case(n),
                SelectItem::Expr { expr, alias: None } => {
                    expr_output_name(expr).is_none_or(|s| !s.eq_ignore_ascii_case(n))
                }
                _ => false,
            })
        })
        .take(5)
        .collect::<Vec<_>>();
    let (channel, key, value, global, key_total) =
        (&slots[0], &slots[1], &slots[2], &slots[3], &slots[4]);
    let mut keys = vec![None; branches.len()];
    let mut replacements = Vec::new();
    let mut key_type = None;
    for leaf in existence_leaves {
        if sources(leaf)
            .iter()
            .any(|t| has_cte(&t.name) || ctes.iter().any(|c| c.name.eq_ignore_ascii_case(&t.name)))
        {
            return None;
        }
        let matches = branches
            .iter()
            .enumerate()
            .filter_map(|(i, b)| match_leaf(db, database, leaf, b).map(|m| (i, m)))
            .collect::<Vec<_>>();
        let [(i, (fk, owner, owner_key))] = matches.as_slice() else {
            return None;
        };
        let (_, c, t) = col(db, database, &branches[*i].select, fk)?;
        let ty = (t.columns[c].data_type.clone(), t.columns[c].unsigned);
        if key_type.as_ref().is_some_and(|old| old != &ty) {
            return None;
        }
        key_type = Some(ty);
        if keys[*i].as_ref().is_some_and(|old| old != fk) {
            return None;
        }
        keys[*i] = Some(fk.clone());
        let mut output = leaf.clone();
        output.from = vec![owner.clone()];
        output.table = Some(owner.name.clone());
        output.joins = vec![Join {
            table: placeholder_table_ref(name.into()),
            join_type: JoinType::Cross,
            straight: false,
            condition: None,
            using_columns: vec![],
            natural: false,
            derived: None,
            lateral: false,
        }];
        let shared = |slot: &str| Expr::CompoundIdentifier(vec![name.into(), slot.into()]);
        output.selection = conjunction(vec![
            eq(shared(channel), integer(*i)),
            eq(shared(key_total), integer(0)),
            eq(shared(key), owner_key.clone()),
        ]);
        for p in &mut output.projection {
            if let SelectItem::Expr { expr, .. } = p {
                let (_, column, schema) = col(db, database, leaf, expr)?;
                *expr = Expr::CompoundIdentifier(vec![
                    qualifier(owner),
                    schema.columns[column].name.clone(),
                ]);
            }
        }
        replacements.push(output);
    }
    let keys = keys.into_iter().collect::<Option<Vec<_>>>()?;
    let mut producer = avg.clone();
    append_source(producer.derived.as_mut()?, &keys, channel, key, &mut 0);
    let aggregate = match &avg.projection[0] {
        SelectItem::Expr { expr, .. } => expr.clone(),
        _ => unreachable!(),
    };
    producer.projection = vec![
        projected(id(channel), channel),
        projected(id(key), key),
        projected(aggregate, value),
        projected(
            Expr::Function {
                name: "GROUPING".into(),
                args: vec![id(channel)],
                distinct: false,
            },
            global,
        ),
        projected(
            Expr::Function {
                name: "GROUPING".into(),
                args: vec![id(key)],
                distinct: false,
            },
            key_total,
        ),
    ];
    producer.group_by = vec![id(channel), id(key)];
    producer.group_by_modifier = GroupByModifier::Rollup;
    let mut consumer = Select::default();
    consumer.from = vec![placeholder_table_ref(name.into())];
    consumer.table = Some(name.into());
    let alias = match &avg.projection[0] {
        SelectItem::Expr { expr, alias } => alias.clone().or_else(|| expr_output_name(expr))?,
        _ => unreachable!(),
    };
    consumer.projection = vec![projected(id(value), &alias)];
    consumer.selection = Some(eq(id(global), integer(1)));
    let mut existence = exists.clone();
    let set = match location {
        None => existence.derived.as_mut()?,
        Some(i) => existence.joins[i].derived.as_mut()?,
    };
    replace_leaves(set, &replacements, &mut 0);
    Some((producer, consumer, existence))
}

pub(super) fn share(db: &Arc<Database>, database: &str, ctes: &mut Vec<CteDef>) {
    let forced = cfg!(test)
        && crate::util::env::var("MYDB_AGGREGATE_EXISTENCE_SHARE").is_ok_and(|v| v == "force");
    if !forced && !crate::util::env::flag("MYDB_AGGREGATE_EXISTENCE_SHARE", true) {
        return;
    }
    let name = (0..)
        .map(|n| format!("__shared_aggregate_{n}"))
        .find(|n| {
            ctes.iter().all(|c| !c.name.eq_ignore_ascii_case(n))
                && db.table_in_database(database, n).is_err()
                && !has_cte(n)
        })
        .unwrap();
    for a in 0..ctes.len() {
        for e in 0..ctes.len() {
            if a == e {
                continue;
            }
            let Some((producer, consumer, existence)) = candidate(db, database, ctes, a, e, &name)
            else {
                continue;
            };
            if !forced && cte_body_scan_volume(db, database, &producer) < 100_000 {
                continue;
            }
            ctes[a].query = consumer.into();
            ctes[e].query = existence.into();
            ctes.insert(
                0,
                CteDef {
                    name,
                    columns: vec![],
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
    fn query(year: &str, tail: &str) -> String {
        format!("WITH matched AS (SELECT owners.id FROM owners,(SELECT o.a aa,o.b bb FROM f1,owners o,calendar d WHERE f1.k=o.id AND f1.d=d.id AND d.y {year} INTERSECT SELECT o.a,o.b FROM f2,owners o,calendar d WHERE f2.k=o.id AND f2.d=d.id AND d.y {year}) keys_found WHERE owners.a=aa AND owners.b=bb), av AS (SELECT AVG(qty*price) average_sales FROM (SELECT n qty,p price FROM f1,calendar WHERE f1.d=calendar.id AND y {year} UNION ALL SELECT n,p FROM f2,calendar WHERE f2.d=calendar.id AND y {year}) inputs) {tail}")
    }
    #[test]
    fn aggregate_existence_share_rejects_observable_bags_and_inexact_inputs() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        for sql in [
            "CREATE TABLE owners(id INT PRIMARY KEY,a INT,b INT)",
            "CREATE TABLE calendar(id INT,y INT)",
            "CREATE TABLE f1(k INT,d INT,n INT,p DECIMAL(24,6))",
            "CREATE TABLE f2(k INT,d INT,n INT,p DECIMAL(24,6))",
        ] {
            engine.execute(sql).unwrap();
        }
        let original = query("BETWEEN 1 AND 2", "SELECT * FROM matched");
        for sql in [
            original.replace("INTERSECT", "UNION ALL"),
            original.replace("AVG(qty*price)", "AVG(SQRT(qty))"),
            original.replace("UNION ALL SELECT n,p", "UNION SELECT n,p"),
            original.replacen("d.y BETWEEN 1 AND 2", "d.y BETWEEN 1 AND 3", 1),
        ] {
            let Statement::WithCte(mut with) = crate::parser::parse_statement(&sql).unwrap() else {
                panic!()
            };
            crate::util::env::with_vars(
                &[("MYDB_AGGREGATE_EXISTENCE_SHARE", Some("force"))],
                || share(engine.db_for_tests(), "test", &mut with.ctes),
            );
            assert_eq!(with.ctes.len(), 2, "unsafe sharing: {sql}");
        }
    }

    #[test]
    fn aggregate_existence_share_preserves_raw_avg_keys_nulls_and_multiplicity() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        for sql in ["CREATE TABLE owners(id INT PRIMARY KEY,a INT,b INT)",
            "CREATE TABLE calendar(id INT,y INT)",
            "CREATE TABLE f1(k INT,d INT,n INT,p DECIMAL(24,6))",
            "CREATE TABLE f2(k INT,d INT,n INT,p DECIMAL(24,6))",
            "INSERT INTO owners VALUES(1,10,20),(2,10,20),(3,NULL,30),(4,40,50)",
            "INSERT INTO calendar VALUES(10,1),(10,1),(20,2)",
            "INSERT INTO f1 VALUES(1,10,3,1.234567),(1,10,1,-1.234568),(2,20,NULL,10),(3,10,2,0.000001),(4,NULL,1,50),(999,10,1,100)",
            "INSERT INTO f2 VALUES(2,10,2,1.234567),(3,10,2,NULL),(4,20,1,10),(NULL,10,1,-2.345678)"] {
            engine.execute(sql).unwrap();
        }
        for year in ["BETWEEN 1 AND 2", "BETWEEN 7 AND 8"] {
            for tail in [
                "SELECT average_sales,(SELECT COUNT(*) FROM matched) n FROM av",
                "SELECT id FROM matched ORDER BY id",
            ] {
                let sql = query(year, tail);
                let run = |gate| {
                    crate::util::env::with_vars(
                        &[("MYDB_AGGREGATE_EXISTENCE_SHARE", Some(gate))],
                        || {
                            let QueryResponse::ResultSet(rs) = engine.execute(&sql).unwrap() else {
                                panic!()
                            };
                            rs
                        },
                    )
                };
                let expected = run("off");
                let actual = run("force");
                assert_eq!(actual.rows, expected.rows, "{sql}");
                assert_eq!(actual.columns, expected.columns, "{sql}");
                if year == "BETWEEN 1 AND 2" && tail.starts_with("SELECT average_sales") {
                    for format in ["TREE", "JSON"] {
                        crate::util::env::with_vars(
                            &[("MYDB_AGGREGATE_EXISTENCE_SHARE", Some("force"))],
                            || {
                                engine
                                    .execute(&format!("EXPLAIN ANALYZE FORMAT={format} {sql}"))
                                    .unwrap();
                            },
                        );
                    }
                }

                let Statement::WithCte(mut with) = crate::parser::parse_statement(&sql).unwrap()
                else {
                    panic!()
                };
                crate::util::env::with_vars(
                    &[("MYDB_AGGREGATE_EXISTENCE_SHARE", Some("force"))],
                    || share(engine.db_for_tests(), "test", &mut with.ctes),
                );
                assert_eq!(with.ctes.len(), 3, "rule missed");
            }
        }
        engine.execute("UPDATE f1 SET p=NULL").unwrap();
        engine.execute("UPDATE f2 SET p=NULL").unwrap();
        let sql = query("BETWEEN 1 AND 2", "SELECT average_sales FROM av");
        let run = |gate| {
            crate::util::env::with_vars(&[("MYDB_AGGREGATE_EXISTENCE_SHARE", Some(gate))], || {
                let QueryResponse::ResultSet(rs) = engine.execute(&sql).unwrap() else {
                    panic!()
                };
                rs
            })
        };
        assert_eq!(run("force").rows, run("off").rows);
    }
}
