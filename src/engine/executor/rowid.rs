//! Shared extraction of exact, table-scoped SQL ROWID candidates.

use super::*;

pub(crate) fn is_pseudo(table: &TableSchema, expr: &Expr) -> bool {
    let name = match expr {
        Expr::Identifier(name) => name,
        Expr::CompoundIdentifier(parts) => match parts.last() {
            Some(name) => name,
            None => return false,
        },
        _ => return false,
    };
    name.eq_ignore_ascii_case("rowid")
        && !table.system
        && table.id != 0
        && (table.find_column_index(name).is_none()
            || crate::storage::rowid::query_column_index(table) == table.find_column_index(name))
}

pub(crate) fn is_reference(table: &TableSchema, alias: Option<&str>, expr: &Expr) -> bool {
    if !is_pseudo(table, expr) {
        return false;
    }
    match expr {
        Expr::Identifier(_) => true,
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            parts[parts.len() - 2].eq_ignore_ascii_case(alias.unwrap_or(&table.name))
                && (parts.len() < 3
                    || (alias.is_none()
                        && parts[parts.len() - 3].eq_ignore_ascii_case(&table.database)))
        }
        _ => false,
    }
}

fn exact_operand(expr: &Expr) -> Option<Option<u64>> {
    match expr_literal_value(expr)? {
        Value::UInt(value) => Some(Some(value)),
        Value::Int(value) => Some(u64::try_from(value).ok()),
        Value::Null => Some(None),
        _ => None,
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct AccessStats {
    pub candidates: u64,
    pub unique: u64,
    pub duplicates_removed: u64,
    pub misses: u64,
    pub fetched: u64,
}

pub(crate) struct Access {
    pub rows: Vec<crate::storage::DmlRowRef>,
    pub stats: AccessStats,
}

struct Addresses {
    values: Vec<u64>,
    duplicates: usize,
}

fn addresses(table: &TableSchema, alias: Option<&str>, expr: &Expr) -> Option<Addresses> {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::Eq | BinaryOp::NullSafeEq,
            right,
        } => {
            let operand = if is_reference(table, alias, left) {
                right
            } else if is_reference(table, alias, right) {
                left
            } else {
                return None;
            };
            Some(Addresses {
                values: exact_operand(operand)?.into_iter().collect(),
                duplicates: 0,
            })
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if is_reference(table, alias, expr) => {
            let mut values = Vec::new();
            for operand in list {
                values.extend(exact_operand(operand)?);
            }
            let count = values.len();
            values.sort_unstable();
            values.dedup();
            let duplicates = count - values.len();
            Some(Addresses { values, duplicates })
        }
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            match (
                addresses(table, alias, left),
                addresses(table, alias, right),
            ) {
                (Some(mut a), Some(b)) => {
                    a.values
                        .retain(|value| b.values.binary_search(value).is_ok());
                    Some(a)
                }
                (a @ Some(_), None) | (None, a @ Some(_)) => a,
                _ => None,
            }
        }
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            let mut a = addresses(table, alias, left)?;
            let b = addresses(table, alias, right)?;
            a.duplicates += b.duplicates;
            a.values.extend(b.values);
            let count = a.values.len();
            a.values.sort_unstable();
            a.values.dedup();
            a.duplicates += count - a.values.len();
            Some(a)
        }
        _ => None,
    }
}

pub(crate) fn access(
    table: &TableSchema,
    alias: Option<&str>,
    expr: &Expr,
) -> Result<Option<Access>, QueryError> {
    let Some(addresses) = addresses(table, alias, expr) else {
        return Ok(None);
    };
    let unique = addresses.values.len() as u64;
    let mut rows = Vec::with_capacity(addresses.values.len());
    for value in addresses.values {
        if let Some(row) = crate::storage::rowid::resolve(table, value)? {
            rows.push(row);
        }
    }
    let stats = AccessStats {
        candidates: unique.saturating_add(addresses.duplicates as u64),
        unique,
        duplicates_removed: addresses.duplicates as u64,
        misses: unique.saturating_sub(rows.len() as u64),
        fetched: 0,
    };
    Ok(Some(Access { rows, stats }))
}

pub(crate) fn candidates_for(
    table: &TableSchema,
    alias: Option<&str>,
    expr: &Expr,
) -> Result<Option<Vec<crate::storage::DmlRowRef>>, QueryError> {
    Ok(access(table, alias, expr)?.map(|access| access.rows))
}

pub(crate) fn candidates(
    table: &TableSchema,
    expr: &Expr,
) -> Result<Option<Vec<crate::storage::DmlRowRef>>, QueryError> {
    crate::storage::rowid::with_dml_alias(|alias| candidates_for(table, alias, expr))
}

/// Compare page-ordered heap probes with a sequential scan, using the same
/// calibrated page/decode costs as other access methods. Unknown statistics
/// must not turn an exact address into an unbounded scan.
pub(crate) fn lookup_cost(
    db: &Database,
    table: &crate::planner::PlanTable,
    rows: &[crate::storage::DmlRowRef],
) -> Result<(crate::engine::cbo::Cost, bool), QueryError> {
    use crate::engine::cbo::{cache_model, consts, operator_cost, CboContext, Cost};
    let population = crate::planner::table_row_estimate(db, table)?;
    let width = operator_cost::estimated_row_width(&table.schema);
    let pages = if table.schema.stats.heap_page_count > 0 {
        table.schema.stats.heap_page_count as f64
    } else {
        operator_cost::estimated_heap_pages(population, width)
    };
    let mut previous = None;
    let distinct_pages = rows
        .iter()
        .filter(|row| {
            let page = (row.storage_id, row.row_id.page_id);
            let different = previous != Some(page);
            previous = Some(page);
            different
        })
        .count() as f64;
    let ctx = CboContext::from_db(db);
    let io = cache_model::page_io_cost(
        cache_model::AccessKind::OrderedNoPrefetch,
        distinct_pages,
        ctx.effective_cache_pages as f64,
        pages,
    );
    let cpu = rows.len() as f64 * operator_cost::per_row_decode_cost(width)
        + distinct_pages * 40.0 * consts::consts().cpu_tuple_cost();
    let lookup = Cost {
        total: io + cpu,
        io_rnd_pages: distinct_pages,
        cpu_units: cpu,
        mem_bytes: rows.len() as f64 * std::mem::size_of::<crate::storage::DmlRowRef>() as f64,
        ..Cost::default()
    };
    let scan = operator_cost::seq_scan(&ctx, pages, population, population, width, 0.0);
    let prefer = rows.len() <= 1
        || table.schema.stats.analyzed_at_unix_ms.is_none()
        || lookup.total <= scan.total;
    Ok((lookup, prefer))
}
