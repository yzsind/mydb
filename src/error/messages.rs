//! Deterministic MySQL 8.0.46 client-message constructors.
//!
//! Parameterized conditions live here so call sites do not independently
//! reimplement punctuation, capitalization, or contractions.

use super::{catalog, QueryError};

pub fn no_such_table(qualified_name: &str) -> QueryError {
    let (schema, table) = crate::storage::naming::split_table_name("", qualified_name);
    catalog::ER_NO_SUCH_TABLE.error(format!("Table '{qualified_name}' doesn't exist"))
        .with_table(&schema, &table)
}

pub fn table_exists(table_name: &str) -> QueryError {
    catalog::ER_TABLE_EXISTS_ERROR.error(format!("Table '{table_name}' already exists"))
}

pub fn bad_null(column_name: &str) -> QueryError {
    catalog::ER_BAD_NULL_ERROR.error(format!("Column '{column_name}' cannot be null"))
        .with_column(column_name)
}

pub fn bad_field(column_name: &str, context: &str) -> QueryError {
    catalog::ER_BAD_FIELD_ERROR.error(format!("Unknown column '{column_name}' in '{context}'"))
        .with_column(column_name)
        .with_details(|details| details.clause = Some(context.to_owned()))
}

pub fn ambiguous_field(column_name: &str, context: &str) -> QueryError {
    catalog::ER_NON_UNIQ_ERROR
        .error(format!("Column '{column_name}' in {context} is ambiguous"))
        .with_column(column_name)
        .with_details(|details| details.clause = Some(context.to_owned()))
}

pub(crate) fn wrong_value_count(schema: &str, table: &str, row: Option<u64>) -> QueryError {
    let error = catalog::ER_WRONG_VALUE_COUNT_ON_ROW
        .error("Column count doesn't match value count")
        .with_table(schema, table);
    match row {
        Some(row) => error.with_input_row(row),
        None => error,
    }
}

pub fn duplicate_entry(value: &str, key_name: &str) -> QueryError {
    catalog::ER_DUP_ENTRY.error(format!("Duplicate entry '{value}' for key '{key_name}'"))
}
