//! Deterministic MySQL 8.0.46 client-message constructors.
//!
//! Parameterized conditions live here so call sites do not independently
//! reimplement punctuation, capitalization, or contractions.

use super::{catalog, QueryError};

pub fn no_such_table(qualified_name: &str) -> QueryError {
    catalog::ER_NO_SUCH_TABLE.error(format!("Table '{qualified_name}' doesn't exist"))
}

pub fn table_exists(table_name: &str) -> QueryError {
    catalog::ER_TABLE_EXISTS_ERROR.error(format!("Table '{table_name}' already exists"))
}

pub fn bad_null(column_name: &str) -> QueryError {
    catalog::ER_BAD_NULL_ERROR.error(format!("Column '{column_name}' cannot be null"))
}

pub fn bad_field(column_name: &str, context: &str) -> QueryError {
    catalog::ER_BAD_FIELD_ERROR.error(format!("Unknown column '{column_name}' in '{context}'"))
}

pub fn duplicate_entry(value: &str, key_name: &str) -> QueryError {
    catalog::ER_DUP_ENTRY.error(format!("Duplicate entry '{value}' for key '{key_name}'"))
}
