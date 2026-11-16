//! Deprecation diagnostics reconstructed when DDL copies catalog metadata.
//!
//! Source-token diagnostics live at the statement boundary, but `LIKE` and
//! CTAS have no deprecated spelling in their own SQL. This module derives the
//! equivalent MySQL warnings from the copied target definition.

#[derive(Clone, Copy)]
pub(super) enum CopiedNumericDeprecation {
    FloatingPointDigits,
    DecimalOrFloatingUnsigned,
    Zerofill,
}

fn for_parts(
    floating_point_digits: bool,
    decimal_or_floating: bool,
    unsigned: bool,
    zerofill: bool,
) -> Vec<CopiedNumericDeprecation> {
    let mut warnings = Vec::with_capacity(2);
    if floating_point_digits {
        warnings.push(CopiedNumericDeprecation::FloatingPointDigits);
    }
    // ZEROFILL implies UNSIGNED in the catalog, but a source declaration
    // containing only ZEROFILL produces one diagnostic rather than two.
    // The catalog intentionally does not retain whether UNSIGNED was also
    // written explicitly, so prefer the non-duplicating interpretation.
    if decimal_or_floating && unsigned && !zerofill {
        warnings.push(CopiedNumericDeprecation::DecimalOrFloatingUnsigned);
    }
    if zerofill {
        warnings.push(CopiedNumericDeprecation::Zerofill);
    }
    warnings
}

pub(super) fn for_column_def(column: &crate::parser::ColumnDef) -> Vec<CopiedNumericDeprecation> {
    use crate::parser::DataType;
    for_parts(
        matches!(
            column.data_type,
            DataType::Float(Some(_)) | DataType::Double(Some(_))
        ),
        matches!(
            column.data_type,
            DataType::Float(_) | DataType::Double(_) | DataType::Decimal(_)
        ),
        column.unsigned,
        column.zerofill,
    )
}

pub(super) fn for_schema(
    column: &crate::storage::schema::ColumnSchema,
) -> Vec<CopiedNumericDeprecation> {
    use crate::storage::schema::DataTypeKind;
    for_parts(
        matches!(
            column.data_type,
            DataTypeKind::Float {
                precision: Some(_),
                scale: Some(_)
            } | DataTypeKind::Double {
                precision: Some(_),
                scale: Some(_)
            }
        ),
        matches!(
            column.data_type,
            DataTypeKind::Float { .. } | DataTypeKind::Double { .. } | DataTypeKind::Decimal { .. }
        ),
        column.unsigned,
        column.zerofill,
    )
}

pub(super) fn emit(warnings: impl IntoIterator<Item = CopiedNumericDeprecation>) {
    for warning in warnings {
        let message = match warning {
            CopiedNumericDeprecation::FloatingPointDigits => "Specifying number of digits for floating point data types is deprecated and will be removed in a future release.",
            CopiedNumericDeprecation::DecimalOrFloatingUnsigned => "UNSIGNED for decimal and floating point data types is deprecated and support for it will be removed in a future release.",
            CopiedNumericDeprecation::Zerofill => "The ZEROFILL attribute is deprecated and will be removed in a future release.",
        };
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT,
            message,
        );
    }
}
