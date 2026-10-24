//! Catalog-free CBO fixtures. Keep synthetic column identity and empty statistics
//! consistent so individual tests only describe the metadata they exercise.
use crate::storage::{DataTypeKind, TableSchema};
use crate::types::Column;

pub(super) fn table_schema(columns: &[(&str, DataTypeKind)]) -> TableSchema {
    let result_columns: Vec<Column> = columns
        .iter()
        .map(|(name, _)| Column {
            name: (*name).to_string(),
            ..Column::default()
        })
        .collect();
    let mut schema = TableSchema::synthetic_for_planning("t", &result_columns);
    schema.id = 1;
    schema.object_id = 1;
    schema.database = "test".to_string();
    schema.schema_version = 1;
    schema.next_column_id = columns.len() as u32 + 1;
    schema.stats.analyzed_at_unix_ms = Some(0);
    schema.row_codec = Default::default();
    for (index, (column, (_, data_type))) in schema.columns.iter_mut().zip(columns).enumerate() {
        column.data_type = data_type.clone();
        column.column_id = index as u32 + 1;
        column.physical_pos = index as u32;
        column.logical_pos = index as u32;
        column.ordinal_position = index as u32 + 1;
        column.charset = None;
        column.collation = None;
    }
    schema
}
