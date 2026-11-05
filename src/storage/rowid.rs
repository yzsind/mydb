//! SQL-visible, table-scoped unsigned physical row addresses.

use super::{error, DmlRowRef, RowId, TableSchema};
use crate::types::QueryError;

/// Query-only column marker. Catalog columns always have a nonzero id.
pub(crate) fn query_column_index(table: &TableSchema) -> Option<usize> {
    table
        .columns
        .last()
        .filter(|column| {
            column.column_id == 0 && column.physical_pos == u32::MAX && column.name == "ROWID"
        })
        .map(|_| table.columns.len() - 1)
}

pub(crate) fn query_schema(table: &TableSchema) -> TableSchema {
    if table.system || table.id == 0 || table.find_column_index("rowid").is_some() {
        return table.clone();
    }
    let mut schema = table.clone();
    let mut column = TableSchema::synthetic_for_planning(
        "",
        &[crate::types::Column {
            name: "ROWID".into(),
            column_type: crate::types::ColumnType::LongLong,
            source: crate::types::ColumnSource::default(),
        }],
    )
    .columns
    .remove(0);
    column.column_id = 0;
    column.physical_pos = u32::MAX;
    column.logical_pos = schema.columns.len() as u32;
    column.unsigned = true;
    column.nullable = false;
    column.invisible = true;
    schema
        .column_name_map
        .insert("rowid".into(), schema.columns.len());
    schema.columns.push(column);
    // The catalog schema and its physical RowCodec are never changed.
    schema
}

thread_local! {
    static PLANNING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static PLANNING_QUALIFIERS: std::cell::RefCell<Option<Vec<String>>> = const { std::cell::RefCell::new(None) };
    static DEMAND: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static DML_ALIAS: std::cell::RefCell<Option<std::sync::Arc<str>>> = const { std::cell::RefCell::new(None) };
    static CURRENT: std::cell::Cell<Option<(u64, u64)>> = const { std::cell::Cell::new(None) };
}

pub(crate) struct DemandScope {
    previous: bool,
    alias: Option<std::sync::Arc<str>>,
}
struct Search(bool);
impl crate::parser::visit::AstVisitor for Search {
    fn enter_expr(&mut self, expr: &crate::parser::Expr) {
        self.0 |= crate::engine::expr::expr_column_name(expr).is_some_and(|name| {
            name.rsplit('.')
                .next()
                .is_some_and(|n| n.eq_ignore_ascii_case("rowid"))
        });
    }
    fn continue_expr_walk(&self) -> bool {
        !self.0
    }
}

impl DemandScope {
    pub(crate) fn enter(statement: &crate::parser::Statement) -> Self {
        let mut search = Search(false);
        if !matches!(statement, crate::parser::Statement::Select(select) if !select.for_update && select.lock_clause.is_none())
        {
            crate::parser::visit::visit_statement(&mut search, statement);
        }
        let alias = if search.0 {
            match statement {
                crate::parser::Statement::Update(stmt) => stmt
                    .primary_alias
                    .as_deref()
                    .map(std::sync::Arc::<str>::from),
                crate::parser::Statement::Delete(stmt) => stmt
                    .primary_alias
                    .as_deref()
                    .map(std::sync::Arc::<str>::from),
                crate::parser::Statement::Select(stmt) => stmt
                    .from
                    .first()
                    .and_then(|table| table.alias.as_deref())
                    .map(std::sync::Arc::<str>::from),
                _ => None,
            }
        } else {
            None
        };
        Self::install(search.0, alias)
    }

    /// UPDATE/DELETE must inspect the rewritten base-table expressions once
    /// view aliases have been expanded. Prepared plans are already rewritten.
    pub(crate) fn enter_before_view_rewrite(statement: &crate::parser::Statement) -> Self {
        if matches!(
            statement,
            crate::parser::Statement::Update(_) | crate::parser::Statement::Delete(_)
        ) {
            Self::install(false, None)
        } else {
            Self::enter(statement)
        }
    }

    pub(crate) fn enter_update(stmt: &crate::parser::Update) -> Self {
        let mut search = Search(false);
        crate::parser::visit::visit_update(&mut search, stmt);
        Self::install(
            search.0,
            stmt.primary_alias
                .as_deref()
                .filter(|_| search.0)
                .map(std::sync::Arc::from),
        )
    }

    pub(crate) fn enter_delete(stmt: &crate::parser::Delete) -> Self {
        let mut search = Search(false);
        crate::parser::visit::visit_delete(&mut search, stmt);
        Self::install(
            search.0,
            stmt.primary_alias
                .as_deref()
                .filter(|_| search.0)
                .map(std::sync::Arc::from),
        )
    }

    fn install(required: bool, alias: Option<std::sync::Arc<str>>) -> Self {
        Self {
            previous: DEMAND.with(|state| state.replace(required)),
            alias: DML_ALIAS.with(|state| state.replace(alias)),
        }
    }
}
impl Drop for DemandScope {
    fn drop(&mut self) {
        DEMAND.with(|state| state.set(self.previous));
        DML_ALIAS.with(|state| *state.borrow_mut() = self.alias.take());
    }
}

pub(crate) fn demanded() -> bool {
    DEMAND.with(|state| state.get())
}

pub(crate) fn with_dml_alias<T>(body: impl FnOnce(Option<&str>) -> T) -> T {
    let alias = DML_ALIAS.with(|state| state.borrow().clone());
    body(alias.as_deref())
}

pub(crate) struct RowScope(Option<Option<(u64, u64)>>);
impl RowScope {
    pub(crate) fn suspend() -> Self {
        Self(Some(CURRENT.with(|state| state.replace(None))))
    }
    pub(crate) fn enter(table: &TableSchema, row: DmlRowRef) -> Result<Self, QueryError> {
        if !DEMAND.with(|state| state.get()) || table.find_column_index("rowid").is_some() {
            return Ok(Self(None));
        }
        let address = for_row(table, row)?;
        Ok(Self(Some(
            CURRENT.with(|state| state.replace(Some((table.id, address)))),
        )))
    }
}
impl Drop for RowScope {
    fn drop(&mut self) {
        if let Some(previous) = self.0 {
            CURRENT.with(|state| state.set(previous));
        }
    }
}

pub(crate) fn current(table: &TableSchema, name: &str) -> Option<crate::types::Value> {
    if !name.eq_ignore_ascii_case("rowid") || table.find_column_index(name).is_some() {
        return None;
    }
    CURRENT.with(|state| {
        state
            .get()
            .filter(|(id, _)| *id == table.id)
            .map(|(_, value)| crate::types::Value::UInt(value))
    })
}

pub(crate) fn current_qualified(
    table: &TableSchema,
    parts: &[String],
) -> Option<crate::types::Value> {
    let name = parts.last()?;
    let matches = with_dml_alias(|alias| {
        parts.len() >= 2
            && parts[parts.len() - 2].eq_ignore_ascii_case(alias.unwrap_or(&table.name))
            && (parts.len() < 3
                || (alias.is_none()
                    && parts[parts.len() - 3].eq_ignore_ascii_case(&table.database)))
    });
    matches.then(|| current(table, name)).flatten()
}

pub(crate) struct PlanningScope(bool, Option<Vec<String>>);

impl PlanningScope {
    pub(crate) fn enter(select: &crate::parser::Select) -> Self {
        #[derive(Default)]
        struct Search {
            unqualified: bool,
            qualifiers: Vec<String>,
        }
        impl crate::parser::visit::AstVisitor for Search {
            fn enter_expr(&mut self, expr: &crate::parser::Expr) {
                match expr {
                    crate::parser::Expr::Identifier(name) if name.eq_ignore_ascii_case("rowid") => {
                        self.unqualified = true
                    }
                    crate::parser::Expr::CompoundIdentifier(parts)
                        if parts
                            .last()
                            .is_some_and(|name| name.eq_ignore_ascii_case("rowid")) =>
                    {
                        if let Some(qualifier) = parts.iter().rev().nth(1) {
                            if !self
                                .qualifiers
                                .iter()
                                .any(|name| name.eq_ignore_ascii_case(qualifier))
                            {
                                self.qualifiers.push(qualifier.clone());
                            }
                        } else {
                            self.unqualified = true;
                        }
                    }
                    _ => {}
                }
            }
            fn continue_expr_walk(&self) -> bool {
                !self.unqualified
            }
        }
        let mut search = Search::default();
        crate::parser::visit::visit_select(&mut search, select);
        let required = search.unqualified || !search.qualifiers.is_empty();
        let qualifiers = (!search.unqualified).then_some(search.qualifiers);
        Self(
            PLANNING.with(|state| state.replace(required)),
            PLANNING_QUALIFIERS.with(|state| state.replace(qualifiers)),
        )
    }
}

impl Drop for PlanningScope {
    fn drop(&mut self) {
        PLANNING.with(|state| state.set(self.0));
        PLANNING_QUALIFIERS.with(|state| *state.borrow_mut() = self.1.take());
    }
}

pub(crate) fn planning_schema(
    table: std::sync::Arc<TableSchema>,
    alias: Option<&str>,
) -> std::sync::Arc<TableSchema> {
    if PLANNING.with(|state| state.get())
        && !table.system
        && table.id != 0
        && table.find_column_index("rowid").is_none()
        && PLANNING_QUALIFIERS.with(|state| {
            state.borrow().as_ref().is_none_or(|names| {
                names
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(alias.unwrap_or(&table.name)))
            })
        })
    {
        std::sync::Arc::new(query_schema(&table))
    } else {
        table
    }
}

#[inline]
pub const fn encode(partition_tag: u16, row: RowId) -> u64 {
    ((partition_tag as u64) << 48) | ((row.page_id as u64) << 16) | row.slot_id as u64
}

#[inline]
pub const fn decode(value: u64) -> (u16, RowId) {
    (
        (value >> 48) as u16,
        RowId::new((value >> 16) as u32, value as u16),
    )
}

pub fn for_row(table: &TableSchema, row: DmlRowRef) -> Result<u64, QueryError> {
    let tag = match &table.partitioning {
        Some(partitions) => partitions.rowid_tag(row.storage_id)?,
        None if row.storage_id == table.id => 0,
        None => return Err(invalid_metadata()),
    };
    Ok(encode(tag, row.row_id))
}

pub(crate) fn append_value(
    table: &TableSchema,
    row: DmlRowRef,
    values: &mut Vec<crate::types::Value>,
) -> Result<(), QueryError> {
    if let Some(index) = query_column_index(table) {
        values.resize_with(index + 1, || crate::types::Value::Null);
        values[index] = crate::types::Value::UInt(for_row(table, row)?);
    }
    Ok(())
}

pub fn resolve(table: &TableSchema, value: u64) -> Result<Option<DmlRowRef>, QueryError> {
    let (tag, row_id) = decode(value);
    if row_id.page_id == 0 {
        return Ok(None);
    }
    let storage_id = match &table.partitioning {
        Some(partitions) => partitions.rowid_storage(tag)?,
        None => (tag == 0).then_some(table.id),
    };
    Ok(storage_id.map(|storage_id| DmlRowRef { storage_id, row_id }))
}

pub(super) fn invalid_metadata() -> QueryError {
    error(
        crate::error::catalog::ER_GET_ERRNO,
        "invalid partition ROWID metadata",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rowid_unsigned_boundaries_round_trip() {
        for tag in [0, 1, 32767, 32768, u16::MAX] {
            for page in [0, 1, u32::MAX] {
                for slot in [0, 1, u16::MAX] {
                    let row = RowId::new(page, slot);
                    assert_eq!(decode(encode(tag, row)), (tag, row));
                }
            }
        }
        assert_eq!(encode(u16::MAX, RowId::new(u32::MAX, u16::MAX)), u64::MAX);
        assert_eq!(encode(0, RowId::new(1, 0)), 65536);
    }
}
