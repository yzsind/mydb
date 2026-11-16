//! Share a pure filtered consumer between a CTE key domain and the final query.
//! The spool retains the full consumer bag; only its domain projection is a set.
use super::*;
use crate::parser::visit::{self, AstVisitor, AstVisitorMut};

fn current_exprs(select: &Select) -> Vec<&Expr> {
    select
        .projection
        .iter()
        .filter_map(|p| match p {
            SelectItem::Expr { expr, .. } => Some(expr),
            _ => None,
        })
        .chain(select.selection.iter())
        .chain(select.having.iter())
        .chain(select.group_by.iter())
        .chain(select.order_by.iter().map(|o| &o.expr))
        .chain(select.joins.iter().filter_map(|j| j.condition.as_ref()))
        .collect()
}

fn uncorrelated(db: &Arc<Database>, current_db: &str, query: &Select, ctes: &[CteDef]) -> bool {
    if query.derived.is_some()
        || query.values.is_some()
        || query.json_table.is_some()
        || query
            .joins
            .iter()
            .any(|j| j.derived.is_some() || j.lateral || j.natural || !j.using_columns.is_empty())
    {
        return false;
    }
    let mut local = HashMap::new();
    for table in query
        .from
        .iter()
        .chain(query.joins.iter().map(|j| &j.table))
    {
        let alias = table_alias_name(table).to_ascii_lowercase();
        let columns = if let Some(cte) = ctes
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(&table.name))
        {
            let Some(names) = existence_cte_output_names(cte) else {
                return false;
            };
            names
        } else {
            let (database, name) = crate::storage::split_table_name(current_db, &table.name);
            let Ok(schema) = db.table_in_database(&database, &name) else {
                return false;
            };
            schema
                .iter_visible_columns()
                .map(|(_, c)| c.name.to_ascii_lowercase())
                .collect()
        };
        if local.insert(alias, columns).is_some() {
            return false;
        }
    }
    current_exprs(query).into_iter().all(|expr| {
        visit::try_inspect_value_expr(expr, |e| {
            match e {
                Expr::Identifier(c)
                    if local
                        .values()
                        .any(|cols| cols.contains(&c.to_ascii_lowercase())) => {}
                Expr::CompoundIdentifier(p)
                    if p.len() == 2
                        && local
                            .get(&p[0].to_ascii_lowercase())
                            .is_some_and(|cols| cols.contains(&p[1].to_ascii_lowercase())) => {}
                Expr::Identifier(_)
                | Expr::CompoundIdentifier(_)
                | Expr::Subquery(_)
                | Expr::InSubquery { .. }
                | Expr::Exists { .. }
                | Expr::QuantifiedComparison { .. }
                | Expr::WindowFunction { .. } => return Err(()),
                _ => {}
            }
            Ok(true)
        })
        .is_ok()
    })
}

/// Only scope-local, deterministic predicates may move into a shared spool.
/// Arithmetic/functions and subqueries retain their original evaluator path.
fn repeatable_filter(expr: &Expr) -> bool {
    visit::try_inspect_value_expr(expr, |e| {
        match e {
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Literal(_)
            | Expr::BoundValue(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::Like { .. } => {}
            Expr::Binary {
                op:
                    BinaryOp::And
                    | BinaryOp::Or
                    | BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq,
                ..
            } => {}
            _ if constant_domain(e).is_some() => return Ok(false),
            _ => return Err(()),
        }
        Ok(true)
    })
    .is_ok()
}

pub(super) fn share(
    db: &Arc<Database>,
    current_db: &str,
    ctes: &mut Vec<CteDef>,
    body: &mut Statement,
) {
    if crate::util::env::var("MYDB_CTE_CONSUMER_SPOOL")
        .is_ok_and(|v| matches!(v.to_ascii_lowercase().as_str(), "0" | "off" | "false"))
    {
        return;
    }
    let Statement::Select(original) = body else {
        return;
    };
    if select_has_nondeterministic(original)
        || ctes
            .iter()
            .any(|c| !statement_is_deterministic_for_cte_cache(&c.query))
    {
        return;
    }
    if original.rls_security_context.is_some() || original.rls_expression_settings.is_some() {
        return;
    }
    let aliases = original
        .projection
        .iter()
        .filter_map(|p| match p {
            SelectItem::Expr {
                alias: Some(alias), ..
            } => Some(alias),
            _ => None,
        })
        .collect::<Vec<_>>();
    if original.having.iter().chain(original.group_by.iter()).chain(original.order_by.iter().map(|o| &o.expr))
        .any(|expr| visit::any_expr(expr, |node| matches!(node, Expr::Identifier(name) if aliases.iter().any(|alias| alias.eq_ignore_ascii_case(name))))) { return; }
    if original.from.is_empty()
        || original.derived.is_some()
        || original.values.is_some()
        || original.json_table.is_some()
        || original.for_update
        || original.lock_clause.is_some()
        || original.sql_calc_found_rows
        || !original.query_hints.is_empty()
        || !original.named_windows.is_empty()
        || !original.grouping_sets.is_empty()
        || original.joins.iter().any(|j| {
            j.derived.is_some()
                || j.lateral
                || j.natural
                || !j.using_columns.is_empty()
                || !matches!(
                    j.join_type,
                    crate::parser::JoinType::Inner | crate::parser::JoinType::Cross
                )
        })
        || original
            .from
            .iter()
            .chain(original.joins.iter().map(|j| &j.table))
            .any(|t| {
                t.system_time.is_some()
                    || t.sample.is_some()
                    || t.generate_series.is_some()
                    || !t.column_aliases.is_empty()
            })
    {
        return;
    }
    let names = ctes
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if original
        .from
        .iter()
        .chain(original.joins.iter().map(|j| &j.table))
        .any(|t| names.contains(&t.name.to_ascii_lowercase()))
    {
        return;
    }
    let Some(selection) = &original.selection else {
        return;
    };
    let mut conjuncts = Vec::new();
    split_and(selection, &mut conjuncts);
    let (dependent, local): (Vec<_>, Vec<_>) = conjuncts
        .into_iter()
        .partition(|e| expr_references_any_cte(e, &names));
    if dependent.is_empty() || local.is_empty() || local.iter().any(|e| !repeatable_filter(e) || !typed_filter(db, current_db, original, e))
        || original.joins.iter().filter_map(|j| j.condition.as_ref()).any(|e| !repeatable_filter(e) || !typed_filter(db, current_db, original, e))
        || dependent.iter().any(|e| !matches!(e, Expr::InSubquery { subquery, negated: false, .. } if uncorrelated(db, current_db, subquery, ctes))) { return; }
    // Require a literal-bearing filter; a mere unrestricted equijoin is not a
    // selective spool candidate. The existing spool enforces its real budget.
    if !local
        .iter()
        .any(|e| visit::any_expr(e, |n| matches!(n, Expr::Literal(_) | Expr::BoundValue(_))))
    {
        return;
    }
    struct Find<'a> {
        original: &'a Select,
        names: &'a [String],
        domains: Vec<Select>,
    }
    impl AstVisitor for Find<'_> {
        fn enter_expr(&mut self, expr: &Expr) {
            let Expr::InSubquery {
                subquery,
                negated: false,
                ..
            } = expr
            else {
                return;
            };
            let [SelectItem::Expr { expr: key, .. }] = subquery.projection.as_slice() else {
                return;
            };
            if build_consumer_domain_select(self.original, key.clone(), self.names)
                .is_some_and(|s| s == **subquery)
                && !self.domains.contains(subquery)
            {
                self.domains.push(subquery.as_ref().clone());
            }
        }
    }
    let mut find = Find {
        original,
        names: &names,
        domains: Vec::new(),
    };
    for cte in ctes.iter() {
        visit::visit_query(&mut find, &cte.query);
    }
    if find.domains.is_empty() {
        return;
    }
    let domains = find.domains;
    let Some(name) = (0..64)
        .map(|i| format!("__mydb_consumer_domain_{i}"))
        .find(|n| !names.contains(n) && db.table_in_database(current_db, n).is_err())
    else {
        return;
    };
    let mut map: HashMap<(String, String), String> = HashMap::new();
    let mut projection = Vec::new();
    let mut rewritten = original.clone();
    rewritten.selection = combine_and(dependent);
    // Projection aliases stay part of the SQL result's public metadata.
    for item in &mut rewritten.projection {
        let SelectItem::Expr { expr, alias } = item else {
            return;
        };
        if alias.is_none() {
            let Some(output) = expr_output_name(expr) else {
                return;
            };
            *alias = Some(output);
        }
    }
    let rewrite = |expr: &mut Expr,
                   map: &mut HashMap<(String, String), String>,
                   projection: &mut Vec<SelectItem>| {
        visit::try_rewrite_value_expr(expr, |node| {
            let (qualifier, column) = match node {
                Expr::Identifier(c) => (None, c.clone()),
                Expr::CompoundIdentifier(p) if p.len() == 2 => (Some(p[0].clone()), p[1].clone()),
                Expr::CompoundIdentifier(_) | Expr::WindowFunction { .. } => return Err(()),
                _ => return Ok(true),
            };
            let Some((alias, _)) =
                resolve_column_ref_table(db, current_db, original, qualifier.as_deref(), &column)
            else {
                return Err(());
            };
            let key = (alias.to_ascii_lowercase(), column.to_ascii_lowercase());
            let output = if let Some(output) = map.get(&key) {
                output.clone()
            } else {
                let output = format!("__c{}", projection.len());
                projection.push(SelectItem::Expr {
                    expr: Expr::CompoundIdentifier(vec![alias, column]),
                    alias: Some(output.clone()),
                });
                map.insert(key, output.clone());
                output
            };
            *node = Expr::CompoundIdentifier(vec![name.clone(), output]);
            Ok(false)
        })
    };
    for item in &mut rewritten.projection {
        if let SelectItem::Expr { expr, .. } = item {
            if rewrite(expr, &mut map, &mut projection).is_err() {
                return;
            }
        }
    }
    for expr in rewritten
        .selection
        .iter_mut()
        .chain(rewritten.having.iter_mut())
        .chain(rewritten.group_by.iter_mut())
        .chain(rewritten.order_by.iter_mut().map(|o| &mut o.expr))
    {
        if rewrite(expr, &mut map, &mut projection).is_err() {
            return;
        }
    }
    let mut replacements = Vec::new();
    for domain in domains {
        let mut replacement = domain.clone();
        let SelectItem::Expr { expr, .. } = &mut replacement.projection[0] else {
            return;
        };
        if rewrite(expr, &mut map, &mut projection).is_err() {
            return;
        }
        replacement.table = Some(name.clone());
        replacement.from = vec![placeholder_table_ref(name.clone())];
        replacement.joins.clear();
        replacement.selection = None;
        replacements.push((domain, replacement));
    }
    let mut producer = original.clone();
    producer.projection = projection;
    producer.selection = combine_and(local);
    producer.group_by.clear();
    producer.grouping_sets.clear();
    producer.group_by_modifier = crate::parser::GroupByModifier::None;
    producer.having = None;
    producer.order_by.clear();
    producer.limit = None;
    producer.offset = None;
    producer.distinct = false;
    producer.named_windows.clear();
    producer.hint_warnings.clear();
    rewritten.table = Some(name.clone());
    rewritten.from = vec![placeholder_table_ref(name.clone())];
    rewritten.joins.clear();
    struct Replace {
        domains: Vec<(Select, Select)>,
    }
    impl AstVisitorMut for Replace {
        fn enter_expr(&mut self, expr: &mut Expr) {
            if let Expr::InSubquery { subquery, .. } = expr {
                if let Some((_, replacement)) = self
                    .domains
                    .iter()
                    .find(|(old, _)| old == subquery.as_ref())
                {
                    **subquery = replacement.clone();
                }
            }
        }
    }
    let mut replace = Replace {
        domains: replacements,
    };
    for cte in ctes.iter_mut() {
        visit::visit_query_mut(&mut replace, &mut cte.query);
    }
    ctes.insert(
        0,
        CteDef {
            name,
            columns: Vec::new(),
            is_recursive: false,
            query: crate::parser::QueryExpr::from(producer),
        },
    );
    *original = rewritten;
}

#[derive(Clone, Copy, PartialEq)]
enum Domain {
    Integer,
    Text,
    Date,
    Null,
}
fn typed_filter(db: &Arc<Database>, current_db: &str, select: &Select, predicate: &Expr) -> bool {
    use crate::storage::DataTypeKind as T;
    let domain = |expr: &Expr| -> Option<Domain> {
        let (qualifier, name) = match expr {
            Expr::Identifier(name) => (None, name.as_str()),
            Expr::CompoundIdentifier(p) if p.len() == 2 => (Some(p[0].as_str()), p[1].as_str()),
            Expr::Literal(crate::parser::Literal::Int(_) | crate::parser::Literal::UInt(_))
            | Expr::BoundValue(Value::Int(_) | Value::UInt(_)) => return Some(Domain::Integer),
            Expr::Literal(crate::parser::Literal::String(_))
            | Expr::BoundValue(Value::String(_) | Value::SmallString(_)) => {
                return Some(Domain::Text)
            }
            Expr::Literal(crate::parser::Literal::Null) | Expr::BoundValue(Value::Null) => {
                return Some(Domain::Null)
            }
            Expr::BoundValue(Value::Date(_)) => return Some(Domain::Date),
            _ => return constant_domain(expr),
        };
        let (_, table) = resolve_column_ref_table(db, current_db, select, qualifier, name)?;
        Some(
            match table.columns[table.column_index(name).ok()?].data_type {
                T::TinyInt | T::SmallInt | T::MediumInt | T::Int | T::BigInt => Domain::Integer,
                T::Char { .. } | T::VarChar { .. } | T::Text { .. } => Domain::Text,
                T::Date => Domain::Date,
                _ => return None,
            },
        )
    };
    let valid_date = |expr: &Expr| {
        let text = match expr {
            Expr::Literal(crate::parser::Literal::String(s))
            | Expr::BoundValue(Value::String(s)) => s.as_str(),
            _ => return false,
        };
        let parts = text.split('-').collect::<Vec<_>>();
        if parts.len() != 3 {
            return false;
        }
        match (
            parts[0].parse::<i32>(),
            parts[1].parse::<u32>(),
            parts[2].parse::<u32>(),
        ) {
            (Ok(y), Ok(m), Ok(d)) => chrono::NaiveDate::from_ymd_opt(y, m, d).is_some(),
            _ => false,
        }
    };
    let compatible = |a: &Expr, b: &Expr| match (domain(a), domain(b)) {
        (Some(a), Some(b)) if a == b || a == Domain::Null || b == Domain::Null => true,
        (Some(Domain::Date), Some(Domain::Text)) => valid_date(b),
        (Some(Domain::Text), Some(Domain::Date)) => valid_date(a),
        _ => false,
    };
    match predicate {
        Expr::Binary {
            left,
            op: BinaryOp::And | BinaryOp::Or,
            right,
        } => {
            typed_filter(db, current_db, select, left)
                && typed_filter(db, current_db, select, right)
        }
        Expr::Binary {
            left,
            op:
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq,
            right,
        } => compatible(left, right),
        Expr::Between {
            expr, low, high, ..
        } => compatible(expr, low) && compatible(expr, high),
        Expr::InList { expr, list, .. } => list.iter().all(|v| compatible(expr, v)),
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => domain(expr).is_some(),
        Expr::Like { expr, pattern, .. } => {
            domain(expr) == Some(Domain::Text) && domain(pattern) == Some(Domain::Text)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::StorageQueryEngine;
    fn engine() -> (tempfile::TempDir, StorageQueryEngine) {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        for sql in [
            "CREATE TABLE domain_facts(order_no BIGINT, marker INT, dk INT, amount DECIMAL(20,2), dt DATE)",
            "CREATE TABLE domain_dim(k INT, tag VARCHAR(8))",
            "CREATE TABLE domain_returns(k BIGINT)",
            "INSERT INTO domain_dim VALUES(1,'keep'),(1,'keep'),(2,'skip')",
            "INSERT INTO domain_facts VALUES(1,1,1,10.00,'2000-03-01'),(1,2,1,15.00,'2000-03-02'),(2,1,1,999.00,'2000-03-01'),(3,NULL,1,888.00,'2000-03-01'),(4,1,1,30.00,'2000-03-01'),(4,2,2,777.00,'2001-01-01'),(NULL,1,1,66.00,'2000-03-01'),(5,1,1,NULL,'2000-03-01'),(5,2,2,NULL,'2001-01-01')",
            "INSERT INTO domain_returns VALUES(1),(1),(4),(5),(NULL)",
        ] { engine.execute(sql).unwrap(); }
        (dir, engine)
    }
    const QUERY: &str = "WITH eligible AS (SELECT a.order_no,a.marker m1,b.marker m2 FROM domain_facts a,domain_facts b WHERE a.order_no=b.order_no AND a.marker<>b.marker) SELECT COUNT(DISTINCT f.order_no) AS cnt,SUM(f.amount) AS total FROM domain_facts f,domain_dim d WHERE f.dk=d.k AND d.tag='keep' AND f.dt BETWEEN '2000-3-01' AND CAST('2000-04-30' AS DATE) AND f.order_no IN (SELECT order_no FROM eligible) AND f.order_no IN (SELECT eligible.order_no FROM domain_returns r,eligible WHERE r.k=eligible.order_no)";
    fn rows(engine: &StorageQueryEngine, sql: &str, enabled: bool) -> crate::engine::ResultSet {
        crate::util::env::with_vars(
            &[(
                "MYDB_CTE_CONSUMER_SPOOL",
                Some(if enabled { "on" } else { "off" }),
            )],
            || {
                let QueryResponse::ResultSet(result) =
                    engine.execute(sql).unwrap().materialize().unwrap()
                else {
                    panic!("rows");
                };
                result
            },
        )
    }
    #[test]
    fn shared_consumer_domain_preserves_bags_nulls_and_outside_domain_markers() {
        let (_dir, engine) = engine();
        let before = rows(&engine, QUERY, false);
        let after = rows(&engine, QUERY, true);
        assert_eq!(before.rows, after.rows);
        assert_eq!(after.rows[0][0], Value::Int(3));
        assert_eq!(before.columns, after.columns);
        let bare = QUERY.replace(
            "COUNT(DISTINCT f.order_no) AS cnt,SUM(f.amount) AS total",
            "f.order_no,f.amount",
        );
        let before_bare = rows(&engine, &bare, false);
        let after_bare = rows(&engine, &bare, true);
        assert_eq!(before_bare.columns, after_bare.columns);
        let mut before_rows = before_bare.rows;
        let mut after_rows = after_bare.rows;
        before_rows.sort_by_key(|r| format!("{r:?}"));
        after_rows.sort_by_key(|r| format!("{r:?}"));
        assert_eq!(before_rows, after_rows);
        let plan = rows(&engine, &format!("EXPLAIN FORMAT=TREE {QUERY}"), true);
        assert!(
            format!("{:?}", plan.rows).contains("__mydb_consumer_domain_"),
            "{:?}",
            plan.rows
        );
    }
    #[test]
    fn shared_consumer_domain_refuses_correlated_and_volatile_filters() {
        let (_dir, engine) = engine();
        for query in [
            QUERY.replace("d.tag='keep'", "RAND()<0.5"),
            format!("{QUERY} ORDER BY total"),
            QUERY.replace(
                "SELECT order_no FROM eligible",
                "SELECT order_no FROM eligible WHERE order_no=f.order_no",
            ),
            QUERY.replace(
                "f.dt BETWEEN '2000-3-01' AND CAST('2000-04-30' AS DATE)",
                "f.amount='bad'",
            ),
            QUERY.replace(
                "f.order_no IN (SELECT order_no",
                "f.order_no NOT IN (SELECT order_no",
            ),
        ] {
            let plan = rows(&engine, &format!("EXPLAIN FORMAT=TREE {query}"), true);
            assert!(
                !format!("{:?}", plan.rows).contains("__mydb_consumer_domain_"),
                "{query}"
            );
        }
    }
}

/// Constant-only builtins may be shared only after a warning-free proof.
/// The original expression remains intact, including its public metadata.
fn constant_domain(expr: &Expr) -> Option<Domain> {
    if !crate::engine::expr::expr_is_constant(expr) || !crate::planner::expr_is_deterministic(expr)
    {
        return None;
    }
    if visit::try_inspect_value_expr(expr, |node| {
        match node {
            Expr::UserVariable(_)
            | Expr::SystemVariable { .. }
            | Expr::Placeholder(_)
            | Expr::Subquery(_)
            | Expr::InSubquery { .. }
            | Expr::Exists { .. }
            | Expr::QuantifiedComparison { .. } => return Err(()),
            Expr::Function { name, .. }
                if name.contains('.')
                    || !(name.eq_ignore_ascii_case("CAST")
                        || crate::engine::cbo::function_cost::is_known_builtin_function(name)) =>
            {
                return Err(())
            }
            _ => {}
        }
        Ok(true)
    })
    .is_err()
    {
        return None;
    }
    let warnings = crate::engine::api::StatementWarningSink::default();
    let value = crate::engine::api::with_statement_warning_sink(warnings.clone(), || {
        crate::engine::expr::eval_const(expr)
    })
    .ok()?;
    if !warnings.lock().is_empty() {
        return None;
    }
    Some(match value {
        Value::Null => Domain::Null,
        Value::Int(_) | Value::UInt(_) => Domain::Integer,
        Value::String(_) | Value::SmallString(_) => Domain::Text,
        Value::Date(_) => Domain::Date,
        _ => return None,
    })
}
