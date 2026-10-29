//! View body normalization for `SHOW CREATE VIEW` / `information_schema.views`.
//!
//! MySQL does not echo the original `CREATE VIEW ... AS <text>` source. It
//! re-prints a *canonical* form of the view body: identifiers backtick-quoted,
//! table references qualified to `` `db`.`tbl` ``, `*` expanded to the explicit
//! column list, every projection given an `AS `alias``, and keywords uppercased.
//!
//! A fully faithful re-printer for every view-eligible construct (correlated
//! subqueries, window functions, `MATCH ... AGAINST` and `GROUP_CONCAT`
//! lowered to internal `__`-prefixed functions, JSON_TABLE, CTE, set ops, …)
//! is a large surface with real correctness hazards — e.g. the parser lowers
//! `MATCH(a) AGAINST('x')` to `__match_against('natural','x','a')`, which a
//! naive printer would emit verbatim and corrupt the view.
//!
//! So this module is deliberately *conservative*: [`normalize_view_body`]
//! returns `Some(canonical_sql)` only for the common, safely-renderable shapes
//! and `None` for anything outside that envelope. Callers fall back to the
//! stored original text when it returns `None`, so the output is never wrong —
//! at worst it is un-normalized, exactly as for a pre-existing catalog written
//! before normalization existed.

use crate::parser::{
    BinaryOp, Expr, Join, JoinType, LimitValue, OrderBy, Select, SelectItem, TableRef, UnaryOp,
};
use crate::storage::split_table_name;
use crate::storage::Database;
use std::sync::Arc;

/// Produce the MySQL-style canonical view body, or `None` if the body uses a
/// construct this normalizer does not faithfully reproduce (the caller then
/// falls back to the original source text). `default_database` is the schema
/// the view lives in; unqualified table names are qualified against it.
pub(crate) fn normalize_view_body(
    db: &Arc<Database>,
    default_database: &str,
    select: &Select,
) -> Option<String> {
    let mut out = String::new();
    render_select(db, default_database, select, &mut out).ok()?;
    Some(out)
}

/// Backtick-quote one identifier, escaping embedded backticks.
fn q(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// `` `db`.`tbl` `` for a base-table / view reference.
fn qualified_relation(default_database: &str, name: &str) -> String {
    let (database, table) = split_table_name(default_database, name);
    format!("{}.{}", q(&database), q(&table))
}

fn render_select(
    db: &Arc<Database>,
    default_database: &str,
    select: &Select,
    out: &mut String,
) -> Result<(), ()> {
    // Bail on any construct we do not render. Falling back to the source
    // text is always correct, so refusing here is the safe default.
    if select.values.is_some()
        || select.json_table.is_some()
        || select.derived.is_some()
        || !select.named_windows.is_empty()
        || !select.grouping_sets.is_empty()
        || select.group_by_modifier != crate::parser::GroupByModifier::None
        || select.sql_calc_found_rows
        || select.straight_join
        || select.for_update
        || select.lock_clause.is_some()
        || !select.query_hints.is_empty()
    {
        return Err(());
    }
    // Derived/lateral/subquery joins are not rendered here.
    for join in &select.joins {
        if join.derived.is_some()
            || join.lateral
            || join.straight
            || join.table.system_time.is_some()
            || !join.table.partitions.is_empty()
            || !join.table.index_hints.is_empty()
        {
            return Err(());
        }
    }
    // FROM must reference resolvable relations with renderable expressions.
    for t in &select.from {
        if t.generate_series.is_some()
            || t.system_time.is_some()
            || !t.partitions.is_empty()
            || !t.index_hints.is_empty()
        {
            return Err(());
        }
    }

    out.push_str("select ");
    if select.distinct {
        out.push_str("distinct ");
    }
    render_projection(db, default_database, select, out)?;

    if !select.from.is_empty() {
        out.push_str(" from ");
        render_from(db, default_database, select, out)?;
    }

    if let Some(sel) = &select.selection {
        out.push_str(" where ");
        out.push_str(&render_expr(db, default_database, sel)?);
    }
    if !select.group_by.is_empty() {
        out.push_str(" group by ");
        let parts: Result<Vec<String>, ()> = select
            .group_by
            .iter()
            .map(|expr| render_expr(db, default_database, expr))
            .collect();
        out.push_str(&parts?.join(","));
    }
    if let Some(having) = &select.having {
        out.push_str(" having ");
        out.push_str(&render_expr(db, default_database, having)?);
    }
    if !select.order_by.is_empty() {
        out.push_str(" order by ");
        out.push_str(&render_order_by(db, default_database, &select.order_by)?);
    }
    render_limit(select, out)?;
    Ok(())
}

fn render_projection(
    db: &Arc<Database>,
    default_database: &str,
    select: &Select,
    out: &mut String,
) -> Result<(), ()> {
    let mut parts: Vec<String> = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard => {
                // Expand `*` to every column of every FROM/JOIN relation,
                // qualified by table alias/name. Only possible when every
                // referenced relation has a known catalog shape. Views expose
                // only their declared columns, never their base-table shape.
                expand_wildcard(db, default_database, select, None, &mut parts)?;
            }
            SelectItem::QualifiedWildcard(qualifier) => {
                expand_wildcard(db, default_database, select, Some(qualifier), &mut parts)?;
            }
            SelectItem::Expr { expr, alias } => {
                let rendered = render_expr(db, default_database, expr)?;
                // MySQL always emits an explicit `AS `alias``; the alias is the
                // user alias when present, otherwise the bare column name for a
                // direct column reference.
                let alias_name = alias.clone().or_else(|| match expr {
                    Expr::Identifier(n) => Some(n.clone()),
                    Expr::CompoundIdentifier(parts) => parts.last().cloned(),
                    _ => None,
                });
                match alias_name {
                    Some(a) => parts.push(format!("{} AS {}", rendered, q(&a))),
                    // An expression with no derivable name (e.g. `1+1`): MySQL
                    // labels it `Name_exp_N`, but reproducing that exactly is
                    // brittle, so bail to the original text instead.
                    None => return Err(()),
                }
            }
        }
    }
    if parts.is_empty() {
        return Err(());
    }
    out.push_str(&parts.join(","));
    Ok(())
}

/// Collect `(alias_or_name, base_table_name)` for every relation in scope, in
/// FROM/JOIN order. Returns `Err` if any relation is not a known base table.
fn scope_relations(
    db: &Arc<Database>,
    default_database: &str,
    select: &Select,
) -> Result<Vec<(String, String)>, ()> {
    let mut rels = Vec::new();
    let mut collect = |t: &TableRef| -> Result<(), ()> {
        let (database, table) = split_table_name(default_database, &t.name);
        if !db.is_base_table(default_database, &t.name)
            && db.view_definition(default_database, &t.name).is_none()
            && crate::engine::information_schema::find_system_table(&database, &table).is_none()
        {
            // Unknown relations cannot supply a stable wildcard shape.
            return Err(());
        }
        let visible = t.alias.clone().unwrap_or_else(|| {
            let (_db, tbl) = split_table_name(default_database, &t.name);
            tbl
        });
        rels.push((visible, t.name.clone()));
        Ok(())
    };
    for t in &select.from {
        collect(t)?;
    }
    for j in &select.joins {
        collect(&j.table)?;
    }
    Ok(rels)
}

fn expand_wildcard(
    db: &Arc<Database>,
    default_database: &str,
    select: &Select,
    qualifier: Option<&str>,
    parts: &mut Vec<String>,
) -> Result<(), ()> {
    let rels = scope_relations(db, default_database, select)?;
    for (visible, base) in &rels {
        if let Some(qual) = qualifier {
            if !visible.eq_ignore_ascii_case(qual) {
                continue;
            }
        }
        if let Some(view) = db.view_definition(default_database, base) {
            for col in &view.columns {
                parts.push(format!("{}.{}", q(visible), q(&col.name)));
            }
            continue;
        }
        let (database, name) = split_table_name(default_database, base);
        if let Some(system) = crate::engine::information_schema::find_system_table(&database, &name)
        {
            for col in system.columns {
                parts.push(format!("{}.{}", q(visible), q(col.name)));
            }
            continue;
        }
        let table = db.table_required(default_database, base).map_err(|_| ())?;
        for (_, col) in table.iter_select_columns() {
            parts.push(format!("{}.{}", q(visible), q(&col.name)));
        }
    }
    Ok(())
}

fn render_from(
    db: &Arc<Database>,
    default_database: &str,
    select: &Select,
    out: &mut String,
) -> Result<(), ()> {
    // First FROM relation.
    let first = &select.from[0];
    render_table_ref(default_database, first, out);
    // Comma-joined additional FROM tables (implicit cross join).
    for t in &select.from[1..] {
        out.push(',');
        render_table_ref(default_database, t, out);
    }
    // Explicit JOINs.
    for join in &select.joins {
        render_join(db, default_database, join, out)?;
    }
    Ok(())
}

fn render_table_ref(default_database: &str, t: &TableRef, out: &mut String) {
    out.push_str(&qualified_relation(default_database, &t.name));
    if let Some(alias) = &t.alias {
        let (_db, tbl) = split_table_name(default_database, &t.name);
        // Only emit the alias when it differs from the bare table name.
        if !alias.eq_ignore_ascii_case(&tbl) {
            out.push(' ');
            out.push_str(&q(alias));
        }
    }
}

fn render_join(
    db: &Arc<Database>,
    default_database: &str,
    join: &Join,
    out: &mut String,
) -> Result<(), ()> {
    if join.natural {
        // NATURAL join canonical rewrite is non-trivial; bail.
        return Err(());
    }
    let kw = match join.join_type {
        JoinType::Inner => " join ",
        JoinType::Left => " left join ",
        JoinType::Right => " right join ",
        JoinType::Cross => " join ",
        // FULL/SEMI/ANTI are not emitted as view source by users; bail.
        JoinType::Full | JoinType::Semi | JoinType::Anti => return Err(()),
    };
    out.push_str(kw);
    render_table_ref(default_database, &join.table, out);
    if !join.using_columns.is_empty() {
        out.push_str(" using (");
        let cols: Vec<String> = join.using_columns.iter().map(|c| q(c)).collect();
        out.push_str(&cols.join(","));
        out.push(')');
    } else if let Some(cond) = &join.condition {
        out.push_str(" on ");
        out.push_str(&render_expr(db, default_database, cond)?);
    }
    Ok(())
}

fn render_order_by(
    db: &Arc<Database>,
    default_database: &str,
    items: &[OrderBy],
) -> Result<String, ()> {
    let mut parts = Vec::with_capacity(items.len());
    for item in items {
        if item.nulls_first.is_some() {
            // Explicit NULLS FIRST/LAST is a mydb extension MySQL rejects; bail
            // so we never emit a non-round-trippable view body.
            return Err(());
        }
        let mut s = render_expr(db, default_database, &item.expr)?;
        if !item.asc {
            s.push_str(" desc");
        }
        parts.push(s);
    }
    Ok(parts.join(","))
}

fn render_limit(select: &Select, out: &mut String) -> Result<(), ()> {
    if [&select.limit, &select.offset]
        .iter()
        .any(|v| matches!(v, Some(value) if !matches!(value, LimitValue::Rows(_))))
    {
        return Err(());
    }
    if let Some(LimitValue::Rows(n)) = &select.limit {
        if let Some(LimitValue::Rows(off)) = &select.offset {
            out.push_str(&format!(" limit {},{}", off, n));
        } else {
            out.push_str(&format!(" limit {}", n));
        }
    }
    Ok(())
}

/// Render an expression to canonical SQL, refusing constructs that the printer
/// cannot reproduce faithfully (placeholders, internal lowered functions,
/// subqueries, window functions, …). Returns `Err(())` to trigger fallback.
fn render_expr(db: &Arc<Database>, default_database: &str, expr: &Expr) -> Result<String, ()> {
    Ok(match expr {
        Expr::Wildcard => "*".to_string(),
        Expr::Identifier(name) => q(name),
        Expr::CompoundIdentifier(parts) => parts.iter().map(|p| q(p)).collect::<Vec<_>>().join("."),
        Expr::Literal(lit) => crate::parser::literal_to_sql_pub(lit),
        Expr::UserVariable(name) => format!("@{name}"),
        Expr::DefaultQualified(parts) => format!(
            "default({})",
            parts
                .iter()
                .map(|part| q(part))
                .collect::<Vec<_>>()
                .join(".")
        ),
        Expr::Default(Some(name)) => format!("default({})", q(name)),
        Expr::Default(None) => "default".to_string(),
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => format!("(not {})", render_expr(db, default_database, expr)?),
            UnaryOp::Neg => format!("-({})", render_expr(db, default_database, expr)?),
            UnaryOp::BitNot => format!("~({})", render_expr(db, default_database, expr)?),
        },
        Expr::Binary {
            left,
            op: BinaryOp::Concat,
            right,
        } => format!(
            "concat({}, {})",
            render_expr(db, default_database, left)?,
            render_expr(db, default_database, right)?
        ),
        Expr::Binary { left, op, right } => format!(
            "({} {} {})",
            render_expr(db, default_database, left)?,
            crate::parser::binary_op_sql_pub(*op),
            render_expr(db, default_database, right)?
        ),
        Expr::IsNull(e) => format!("({} is null)", render_expr(db, default_database, e)?),
        Expr::IsNotNull(e) => format!("({} is not null)", render_expr(db, default_database, e)?),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let items: Result<Vec<String>, ()> = list
                .iter()
                .map(|expr| render_expr(db, default_database, expr))
                .collect();
            format!(
                "({} {}in ({}))",
                render_expr(db, default_database, expr)?,
                if *negated { "not " } else { "" },
                items?.join(",")
            )
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => format!(
            "({} {}between {} and {})",
            render_expr(db, default_database, expr)?,
            if *negated { "not " } else { "" },
            render_expr(db, default_database, low)?,
            render_expr(db, default_database, high)?
        ),
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
        } => {
            let mut s = format!(
                "({} {}like {}",
                render_expr(db, default_database, expr)?,
                if *negated { "not " } else { "" },
                render_expr(db, default_database, pattern)?
            );
            s.push_str(" escape ");
            s.push_str(&crate::parser::literal_to_sql_pub(
                &crate::parser::Literal::String(
                    escape_char.map_or_else(String::new, |ch| ch.to_string()),
                ),
            ));
            s.push(')');
            s
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let mut s = String::from("(case");
            if let Some(op) = operand {
                s.push(' ');
                s.push_str(&render_expr(db, default_database, op)?);
            }
            for (cond, res) in conditions.iter().zip(results.iter()) {
                s.push_str(" when ");
                s.push_str(&render_expr(db, default_database, cond)?);
                s.push_str(" then ");
                s.push_str(&render_expr(db, default_database, res)?);
            }
            if let Some(e) = else_result {
                s.push_str(" else ");
                s.push_str(&render_expr(db, default_database, e)?);
            }
            s.push_str(" end)");
            s
        }
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            // Internal lowered functions (`__match_against`, `__gc_ordered`,
            // `__hidden_first_value`, `__interval`, …) must never be printed —
            // they would not round-trip. Bail to source text.
            if crate::parser::IntrinsicFunction::from_name(name).is_some()
                || (name.eq_ignore_ascii_case("JSON_VALUE") && args.len() > 2)
            {
                return Err(());
            }
            // TRIM is lowered to (input, removal, direction), but SQL uses FROM.
            if name.eq_ignore_ascii_case("TRIM") && args.len() == 3 {
                let Expr::Literal(crate::parser::Literal::String(direction)) = &args[2] else {
                    return Err(());
                };
                if !matches!(direction.as_str(), "BOTH" | "LEADING" | "TRAILING") {
                    return Err(());
                }
                return Ok(format!(
                    "TRIM({direction} {} FROM {})",
                    render_expr(db, default_database, &args[1])?,
                    render_expr(db, default_database, &args[0])?,
                ));
            }
            let rendered: Result<Vec<String>, ()> = args
                .iter()
                .map(|expr| render_expr(db, default_database, expr))
                .collect();
            format!(
                "{}({}{})",
                name,
                if *distinct { "distinct " } else { "" },
                rendered?.join(",")
            )
        }
        Expr::Subquery(select) => {
            let mut sql = String::new();
            render_select(db, default_database, select, &mut sql)?;
            format!("({sql})")
        }
        Expr::Exists { subquery, negated } => {
            let mut sql = String::new();
            render_select(db, default_database, subquery, &mut sql)?;
            format!("({}exists ({sql}))", if *negated { "not " } else { "" })
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let mut sql = String::new();
            render_select(db, default_database, subquery, &mut sql)?;
            format!(
                "({} {}in ({sql}))",
                render_expr(db, default_database, expr)?,
                if *negated { "not " } else { "" }
            )
        }
        // Everything else — window functions,
        // quantified comparisons, tuples, placeholders, bound values, group-key
        // refs, grouping — is not faithfully renderable here. Fall back.
        _ => return Err(()),
    })
}

#[cfg(test)]
mod audit_tests {
    use super::*;

    #[test]
    fn unresolved_limit_cannot_disappear_from_canonical_view() {
        let crate::parser::Statement::Select(mut select) =
            crate::parser::parse_statement("SELECT 1 LIMIT 5").unwrap()
        else {
            panic!()
        };
        let mut out = String::new();
        select.limit = Some(LimitValue::Placeholder(0));
        assert!(render_limit(&select, &mut out).is_err());
        assert!(out.is_empty());
        select.limit = Some(LimitValue::Rows(5));
        select.offset = Some(LimitValue::Placeholder(0));
        assert!(render_limit(&select, &mut out).is_err());
        assert!(out.is_empty());
    }
}
