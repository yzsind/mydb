/// True when the active session's sql_mode contains
/// `TIME_TRUNCATE_FRACTIONAL` — excess fractional seconds are then
/// truncated instead of rounded (MySQL 8.0 semantics). Read from the
/// expression layer's thread-local session view; defaults to rounding
/// when no session is installed.
#[inline]
fn temporal_mode_truncates_fractional() -> bool {
    crate::engine::expr::session_temporal_sql_mode_bits()
        & crate::engine::api::TEMPORAL_MODE_TIME_TRUNCATE_FRACTIONAL
        != 0
}

/// Temporal slice of the unified `dml_violation` outlet (dml design doc
/// Q1): in strict sql_mode the violation is an error (1292); otherwise
/// it is recorded as a statement warning and the caller stores the
/// MySQL fallback value (the zero date) instead.
fn temporal_violation(msg: String) -> Result<(), QueryError> {
    if coercion_strict() {
        Err(error(crate::error::catalog::ER_TRUNCATED_WRONG_VALUE, msg))
    } else {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
            msg,
        );
        Ok(())
    }
}

/// Why a date-shaped string failed to parse — drives the sql_mode bits.
enum BadDateShape {
    /// `2009-00-15` / `2009-01-00`: zero month or day part.
    ZeroInDate,
    /// `2009-02-31`: parts in range but not a real calendar date.
    InvalidCalendar,
    /// Anything else (not date-shaped at all).
    Garbage,
}

fn classify_bad_date(s: &str) -> BadDateShape {
    match parse_ymd_fast(s.trim().as_bytes()) {
        Some((y, m, d)) => {
            if (m == 0 || d == 0) && !(y == 0 && m == 0 && d == 0) {
                BadDateShape::ZeroInDate
            } else if (1..=12).contains(&m) && (1..=31).contains(&d) {
                BadDateShape::InvalidCalendar
            } else {
                BadDateShape::Garbage
            }
        }
        None => BadDateShape::Garbage,
    }
}

/// Decide whether a component-shaped invalid date must be preserved.
/// MySQL's ALLOW_INVALID_DATES applies to DATE/DATETIME (not TIMESTAMP);
/// zero month/day is independently controlled by NO_ZERO_IN_DATE.
/// `Ok(None)` means the non-strict warning path should store the zero value.
fn invalid_temporal_components(
    s: &str,
    what: &str,
    allow_tagged: bool,
) -> Result<Option<(u32, u32, u32)>, QueryError> {
    let bits = crate::engine::expr::session_temporal_sql_mode_bits();
    let parts = parse_ymd_fast(s.trim().as_bytes())
        .and_then(|(year, month, day)| (year >= 0).then_some((year as u32, month, day)));
    match classify_bad_date(s) {
        BadDateShape::ZeroInDate
            if allow_tagged && bits & crate::engine::api::TEMPORAL_MODE_NO_ZERO_IN_DATE == 0 =>
        {
            return Ok(parts);
        }
        BadDateShape::InvalidCalendar
            if allow_tagged
                && bits & crate::engine::api::TEMPORAL_MODE_ALLOW_INVALID_DATES != 0 =>
        {
            return Ok(parts);
        }
        _ => {}
    }
    temporal_violation(format!("incorrect {what} value: '{s}'"))?;
    Ok(None)
}

fn parse_tagged_datetime(s: &str) -> Option<(u32, u32, u32, u32)> {
    let bytes = s.trim().as_bytes();
    if bytes.len() == 10 {
        return Some((0, 0, 0, 0));
    }
    if bytes.len() < 19
        || !matches!(bytes[10], b' ' | b'T')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let two = |slice: &[u8]| -> Option<u32> {
        (slice.len() == 2 && slice.iter().all(u8::is_ascii_digit))
            .then(|| ((slice[0] - b'0') as u32) * 10 + (slice[1] - b'0') as u32)
    };
    let hour = two(&bytes[11..13])?;
    let minute = two(&bytes[14..16])?;
    let second = two(&bytes[17..19])?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let mut micros = 0u32;
    if bytes.len() > 19 {
        if bytes[19] != b'.' || bytes[20..].is_empty() {
            return None;
        }
        let mut digits = 0u32;
        for &byte in &bytes[20..] {
            if !byte.is_ascii_digit() {
                return None;
            }
            if digits < 6 {
                micros = micros * 10 + (byte - b'0') as u32;
                digits += 1;
            }
        }
        while digits < 6 {
            micros *= 10;
            digits += 1;
        }
    }
    Some((hour, minute, second, micros))
}

/// `NO_ZERO_DATE` policy for an explicitly-written zero date. Errors in
/// strict mode, warns otherwise; a no-op when the bit is unset.
fn zero_date_policy(what: &str) -> Result<(), QueryError> {
    let bits = crate::engine::expr::session_temporal_sql_mode_bits();
    if bits & crate::engine::api::TEMPORAL_MODE_NO_ZERO_DATE != 0 {
        if DML_IGNORE_COERCION_DEPTH.with(|depth| depth.get() != 0) {
            // INSERT IGNORE converts NO_ZERO_DATE's invalid temporal
            // condition to the assignment-domain out-of-range warning, not
            // the expression-domain 1292 warning. The zero sentinel is still
            // stored so legacy NOT NULL DATE lookups remain observable.
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                format!("Out of range value for {what} column"),
            );
            return Ok(());
        }
        temporal_violation(format!(
            "incorrect {what} value: '0000-00-00' (NO_ZERO_DATE)"
        ))?;
    }
    Ok(())
}

fn coerce_row(table: &TableSchema, values: &mut [Value]) -> Result<(), QueryError> {
    let _engine_policy = NonTransactionalCoercionGuard::new(&table.engine);
    if values.len() != table.columns.len() {
        return Err(error(
            crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
            "column count does not match value count",
        ));
    }
    for (value, column) in values.iter_mut().zip(&table.columns) {
        let current = std::mem::replace(value, Value::Null);
        *value = coerce_column_value_for_schema(current, column)?;
    }
    validate_cosine_index_vectors(table, values)?;
    Ok(())
}

pub(crate) fn coerce_updated_row(
    table: &TableSchema,
    values: &mut [Value],
    changed_columns: Option<&[usize]>,
) -> Result<(), QueryError> {
    let _engine_policy = NonTransactionalCoercionGuard::new(&table.engine);
    if values.len() != table.columns.len() {
        return Err(error(
            crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
            "column count does not match value count",
        ));
    }
    if !coercion_strict() {
        for (idx, column) in table.columns.iter().enumerate() {
            if changed_columns.is_some_and(|changed| !changed.contains(&idx))
                || column.nullable
                || column.generated_expr.is_some()
                || !matches!(values[idx], Value::Null)
            {
                continue;
            }
            let Some(default) =
                crate::engine::executor::insert::implicit_default_for_type(&column.data_type)
            else {
                return Err(crate::error::messages::bad_null(&column.name));
            };
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_BAD_NULL_ERROR,
                format!("Column '{}' cannot be null", column.name),
            );
            values[idx] = default;
        }
    }
    let Some(changed_columns) = changed_columns else {
        return coerce_row(table, values);
    };
    for idx in changed_columns.iter().copied() {
        let Some(column) = table.columns.get(idx) else {
            continue;
        };
        let current = std::mem::replace(&mut values[idx], Value::Null);
        values[idx] = coerce_column_value_for_schema(current, column)?;
    }
    compact_char_columns_for_storage(table, values);
    validate_cosine_index_vectors(table, values)?;
    Ok(())
}

fn validate_cosine_index_vectors(table: &TableSchema, values: &[Value]) -> Result<(), QueryError> {
    for index in table.indexes.iter().filter(|index| {
        index.is_vector()
            && index.vector_meta().metric_enum() == crate::engine::vector::Metric::Cosine
    }) {
        let col_idx = table.column_index(&index.columns[0])?;
        if let Some(Value::Vector(bytes)) = values.get(col_idx) {
            if crate::engine::vector::distance::typed_is_zero(bytes) {
                return Err(error(
                    crate::error::catalog::MYDB_ER_INVALID_VECTOR_VALUE,
                    format!(
                        "zero vector is not valid for cosine vector index '{}'",
                        index.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn compact_char_columns_for_storage(table: &TableSchema, values: &mut [Value]) {
    for (value, column) in values.iter_mut().zip(table.columns.iter()) {
        if !matches!(column.data_type, DataTypeKind::Char { .. }) {
            continue;
        }
        match value {
            Value::String(text) | Value::Decimal(text) => {
                let compact_len = crate::util::pad_space::trim_trailing_spaces_len(text.as_bytes());
                text.truncate(compact_len);
            }
            Value::Bytes(bytes) => {
                let compact_len = crate::util::pad_space::trim_trailing_spaces_len(bytes);
                bytes.truncate(compact_len);
            }
            _ => {}
        }
    }
}

pub(crate) fn index_may_be_affected_by_columns(
    table: &TableSchema,
    index: &IndexSchema,
    changed_columns: Option<&[usize]>,
) -> bool {
    let Some(changed_columns) = changed_columns else {
        return true;
    };
    // Declared key parts: a column part is affected when the heap slot
    // it reads changed; a functional part is affected when *any* of the
    // columns its expression references changed. INCLUDE columns are
    // appended verbatim to the key, so they're treated like column key
    // parts.
    let key_part_changed = (0..index.columns.len()).any(|pos| match index.key_part(pos) {
        Some(crate::storage::IndexKeyPart::Column(name)) => match table.column_index(name) {
            Ok(idx) => changed_columns.contains(&idx),
            // Unknown column name shouldn't happen for a Public index;
            // fall back to "assume affected" so we never miss an
            // index repair.
            Err(_) => true,
        },
        Some(crate::storage::IndexKeyPart::Expression(expr)) => {
            expr.referenced_columns
                .iter()
                .any(|name| match table.column_index(name) {
                    Ok(idx) => changed_columns.contains(&idx),
                    Err(_) => true,
                })
        }
        None => true,
    });
    if key_part_changed {
        return true;
    }
    index
        .included_columns
        .iter()
        .any(|column| match table.column_index(column) {
            Ok(idx) => changed_columns.contains(&idx),
            Err(_) => true,
        })
}

pub(crate) fn expand_changed_columns_for_generated(
    table: &TableSchema,
    changed_columns: Option<&[usize]>,
) -> Option<Vec<usize>> {
    let changed_columns = changed_columns?;
    let mut expanded = changed_columns.to_vec();
    for (idx, column) in table.columns.iter().enumerate() {
        if column.generated_expr.is_none() {
            continue;
        }
        if generated_column_references_any(column, &expanded, table) && !expanded.contains(&idx) {
            expanded.push(idx);
        }
    }
    Some(expanded)
}

fn materialize_generated_columns(
    table: &TableSchema,
    values: &mut [Value],
    changed_columns: Option<&[usize]>,
) -> Result<(), QueryError> {
    if values.len() != table.columns.len() {
        return Err(error(
            crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
            "column count does not match value count",
        ));
    }
    if !table
        .columns
        .iter()
        .any(|column| column.generated_expr.is_some())
    {
        return Ok(());
    }
    // Generated expressions observe column domains, never unconverted INSERT
    // literals. Normalize base fields before evaluating any dependent column.
    let _engine_policy = NonTransactionalCoercionGuard::new(&table.engine);
    for (value, column) in values.iter_mut().zip(&table.columns) {
        if column.generated_expr.is_none() {
            let current = std::mem::replace(value, Value::Null);
            *value = coerce_column_value_for_schema(current, column)?;
        }
    }
    // MySQL row-based replication carries every STORED generated column's
    // authoritative value in the after-image; the source is single-source-of-
    // truth for that byte pattern (expr evaluation on the replica can diverge
    // from the source under differing sql_mode/tz/function versions). Only
    // VIRTUAL generated columns must still be (re)computed on the replica —
    // they are not in the row image and mydb's schema keeps a slot for them.
    let replica_authoritative_stored = crate::replication::replication_applier_active();
    for (idx, column) in table.columns.iter().enumerate() {
        if column.generated_expr.is_none() {
            continue;
        }
        if replica_authoritative_stored && column.generated_stored {
            continue;
        }
        if changed_columns.is_some_and(|columns| !columns.contains(&idx)) {
            continue;
        }
        let value = generated_column_value(table, idx, values)?;
        values[idx] = value;
    }
    Ok(())
}

fn generated_column_value(
    table: &TableSchema,
    idx: usize,
    values: &[Value],
) -> Result<Value, QueryError> {
    let column = table.columns.get(idx).ok_or_else(|| {
        error(
            crate::error::catalog::ER_BAD_FIELD_ERROR,
            "unknown generated column",
        )
    })?;
    let program = column.generated_program.as_ref().ok_or_else(|| {
        error(
            crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
            format!("generated column '{}' is not executable", column.name),
        )
    })?;
    let value = program.eval(table, values)?;
    coerce_column_value_for_schema(value, column)
}

fn generated_column_references_any(
    generated: &ColumnSchema,
    columns: &[usize],
    table: &TableSchema,
) -> bool {
    columns.iter().any(|idx| {
        table.columns.get(*idx).is_some_and(|column| {
            generated
                .generated_column_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&column.name))
        })
    })
}

fn check_constraint_error(name: &str) -> QueryError {
    error(
        crate::error::catalog::ER_CHECK_CONSTRAINT_VIOLATED,
        format!("Check constraint '{name}' is violated."),
    )
}

fn build_check_constraints_for_table(
    table: &TableSchema,
    defs: Vec<crate::parser::CheckConstraintDef>,
    existing: Option<&[CheckConstraintSchema]>,
    preferred_start: Option<usize>,
) -> Result<Vec<CheckConstraintSchema>, QueryError> {
    let mut used = existing
        .unwrap_or(&[])
        .iter()
        .map(|check| check.name.clone())
        .collect::<BTreeSet<_>>();
    let mut out = Vec::with_capacity(defs.len());
    let mut next_generated = preferred_start.unwrap_or(1);
    for def in defs {
        // Clone `def.name` so the later `&def` borrow is not blocked
        // by partially moving the option during name selection.
        let name = match def.name.clone() {
            Some(name) => {
                validate_check_constraint_name(&name)?;
                name
            }
            None => loop {
                let candidate = format!("{}_chk_{}", table.name, next_generated);
                next_generated = next_generated.saturating_add(1);
                if !used.contains(&candidate) {
                    break candidate;
                }
            },
        };
        if !used.insert(name.clone()) {
            return Err(error(
                crate::error::catalog::ER_CHECK_CONSTRAINT_DUP_NAME,
                format!("Duplicate check constraint name '{name}'."),
            ));
        }
        let column_names = check_constraint_referenced_columns(table, &def)?;
        let expr = encode_catalog_expr(&def.expr)?;
        validate_check_expr_supported(table, &def.expr)?;
        out.push(CheckConstraintSchema {
            name,
            expr,
            enforced: def.enforced,
            column_names,
            expr_ast: None,
            program: None,
        });
    }
    Ok(out)
}

fn check_constraint_names_in_schema(
    catalog: &Catalog,
    database: &str,
    excluding_table_key: Option<&str>,
) -> BTreeSet<String> {
    catalog
        .tables
        .iter()
        .filter(|(key, table)| {
            table.database.eq_ignore_ascii_case(database)
                && excluding_table_key.is_none_or(|excluded| key.as_str() != excluded)
        })
        .flat_map(|(_, table)| {
            table
                .check_constraints
                .iter()
                .map(|check| check.name.clone())
        })
        .collect()
}

fn ensure_check_constraint_names_available(
    used: &BTreeSet<String>,
    candidates: &[CheckConstraintSchema],
) -> Result<(), QueryError> {
    let mut seen = BTreeSet::new();
    for check in candidates {
        if !seen.insert(check.name.clone()) || used.contains(&check.name) {
            return Err(error(
                crate::error::catalog::ER_CHECK_CONSTRAINT_DUP_NAME,
                format!("Duplicate check constraint name '{}'.", check.name),
            ));
        }
    }
    Ok(())
}

fn validate_check_constraint_name(name: &str) -> Result<(), QueryError> {
    if name.is_empty() || name.len() > 64 {
        return Err(error(
            crate::error::catalog::ER_TOO_LONG_IDENT,
            "identifier name is too long for a check constraint",
        ));
    }
    Ok(())
}

fn check_constraint_referenced_columns(
    table: &TableSchema,
    def: &crate::parser::CheckConstraintDef,
) -> Result<Vec<String>, QueryError> {
    let mut columns = BTreeSet::new();
    collect_check_expr_columns(table, &def.expr, &mut columns)?;
    if let Some(column) = &def.column {
        for referenced in &columns {
            if !referenced.eq_ignore_ascii_case(column) {
                return Err(error(
                    crate::error::catalog::ER_COLUMN_CHECK_CONSTRAINT_REFERENCES_OTHER_COLUMN,
                    "column CHECK constraint cannot refer to another column",
                ));
            }
        }
    }
    Ok(columns.into_iter().collect())
}

fn reject_auto_increment_check_column(column: &ColumnSchema) -> Result<(), QueryError> {
    if column.auto_increment {
        return Err(error(
            crate::error::catalog::ER_CHECK_CONSTRAINT_FUNCTION_IS_NOT_ALLOWED,
            "CHECK constraint cannot refer to an AUTO_INCREMENT column",
        ));
    }
    Ok(())
}

fn collect_check_expr_columns(
    table: &TableSchema,
    expr: &Expr,
    out: &mut BTreeSet<String>,
) -> Result<(), QueryError> {
    crate::parser::visit::try_for_each_expr(expr, |expr| {
        match expr {
            Expr::Identifier(name) => {
                let idx = table.column_index(name)?;
                reject_auto_increment_check_column(&table.columns[idx])?;
                out.insert(table.columns[idx].name.clone());
            }
            Expr::CompoundIdentifier(parts) => {
                let Some(column) = parts.last() else {
                    return Err(error(
                        crate::error::catalog::ER_BAD_FIELD_ERROR,
                        "unknown column",
                    ));
                };
                if parts.len() > 2 {
                    return Err(error(
                        crate::error::catalog::ER_CHECK_CONSTRAINT_FUNCTION_IS_NOT_ALLOWED,
                        "CHECK constraint cannot refer to columns outside the table",
                    ));
                }
                if parts.len() == 2 && !parts[0].eq_ignore_ascii_case(&table.name) {
                    return Err(error(
                        crate::error::catalog::ER_CHECK_CONSTRAINT_FUNCTION_IS_NOT_ALLOWED,
                        "CHECK constraint cannot refer to columns outside the table",
                    ));
                }
                let idx = table.column_index(column)?;
                reject_auto_increment_check_column(&table.columns[idx])?;
                out.insert(table.columns[idx].name.clone());
            }
            Expr::Function { name, distinct, .. } => {
                if crate::storage::schema::is_nondeterministic_function(name) {
                    return Err(error(
                        crate::error::catalog::ER_CHECK_CONSTRAINT_NAMED_FUNCTION_IS_NOT_ALLOWED,
                        format!("CHECK constraint contains disallowed function: {name}"),
                    ));
                }
                if *distinct || crate::planner::is_aggregate_function(name) {
                    return Err(error(
                        crate::error::catalog::ER_CHECK_CONSTRAINT_FUNCTION_IS_NOT_ALLOWED,
                        "CHECK constraint expression cannot contain aggregate functions",
                    ));
                }
            }
            Expr::Unary { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::Binary { .. }
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::Case { .. }
            | Expr::Tuple(_)
            | Expr::GroupKeyRef { .. } => {}
            Expr::Literal(_) | Expr::BoundValue(_) => {}
            Expr::Wildcard
            | Expr::UserVariable(_)
            | Expr::SystemVariable { .. }
            | Expr::Placeholder(_)
            | Expr::Default(_)
            | Expr::DefaultQualified(_)
            | Expr::Grouping(_)
            | Expr::Subquery(_)
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
            | Expr::Exists { .. }
            | Expr::WindowFunction { .. } => {
                return Err(error(
                    crate::error::catalog::ER_CHECK_CONSTRAINT_FUNCTION_IS_NOT_ALLOWED,
                    "unsupported expression in CHECK constraint",
                ));
            }
        }
        Ok(true)
    })
}

fn validate_check_expr_supported(table: &TableSchema, expr: &Expr) -> Result<(), QueryError> {
    let schema = crate::engine::expr_program::InputSchema::from_table(table);
    crate::engine::expr_program::ExprCompiler::compile_check_constraint(expr, &schema)
        .map(|_| ())
        .map_err(|_| {
            error(
                crate::error::catalog::ER_CHECK_CONSTRAINT_FUNCTION_IS_NOT_ALLOWED,
                "unsupported expression in CHECK constraint",
            )
        })
}

fn validate_and_prepare_generated_columns(table: &mut TableSchema) -> Result<(), QueryError> {
    let generated_exprs = table
        .columns
        .iter()
        .map(|column| {
            column
                .generated_expr
                .as_deref()
                .map(crate::parser::parse_catalog_expr)
                .transpose()
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                        format!(
                            "generated column '{}' expression is not parseable",
                            column.name
                        ),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let schema = crate::engine::expr_program::InputSchema::from_table(table);
    let mut referenced_by_column = Vec::with_capacity(table.columns.len());
    for (idx, expr) in generated_exprs.iter().enumerate() {
        let Some(expr) = expr else {
            referenced_by_column.push(Vec::new());
            continue;
        };
        let mut columns = BTreeSet::new();
        collect_generated_expr_columns(table, idx, expr, &mut columns)?;
        crate::engine::expr_program::ExprCompiler::compile_strict(expr, &schema).map_err(|_| {
            if let Expr::Function { name, .. } = expr {
                error(
                    crate::error::catalog::ER_GENERATED_COLUMN_NAMED_FUNCTION_IS_NOT_ALLOWED,
                    format!(
                        "Expression of generated column '{}' contains a disallowed function: {name}.",
                        table.columns[idx].name
                    ),
                )
            } else {
                error(crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                    format!(
                        "unsupported expression in generated column '{}'",
                        table.columns[idx].name
                    ),
                )
            }
        })?;
        referenced_by_column.push(columns.into_iter().collect::<Vec<_>>());
    }
    for (column, names) in table.columns.iter_mut().zip(referenced_by_column) {
        column.generated_column_names = names;
    }
    Ok(())
}

fn collect_generated_expr_columns(
    table: &TableSchema,
    generated_idx: usize,
    expr: &Expr,
    out: &mut BTreeSet<String>,
) -> Result<(), QueryError> {
    crate::parser::visit::try_for_each_expr(expr, |expr| {
        match expr {
            Expr::Identifier(name) => {
                collect_generated_expr_column(table, generated_idx, name, out)?;
            }
            Expr::CompoundIdentifier(parts) => {
                let Some(column) = parts.last() else {
                    return Err(error(
                        crate::error::catalog::ER_BAD_FIELD_ERROR,
                        "unknown column",
                    ));
                };
                if parts.len() > 2 {
                    return Err(error(
                        crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                        "generated column expression cannot refer to columns outside the table",
                    ));
                }
                if parts.len() == 2 && !parts[0].eq_ignore_ascii_case(&table.name) {
                    return Err(error(
                        crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                        "generated column expression cannot refer to columns outside the table",
                    ));
                }
                collect_generated_expr_column(table, generated_idx, column, out)?;
            }
            Expr::Function { name, distinct, .. } => {
                if crate::engine::http::is_function(name)
                    || name.eq_ignore_ascii_case("STATEMENT_DIGEST")
                    || name.eq_ignore_ascii_case("STATEMENT_DIGEST_TEXT")
                {
                    return Err(error(
                    crate::error::catalog::ER_GENERATED_COLUMN_NAMED_FUNCTION_IS_NOT_ALLOWED,
                    format!(
                        "Expression of generated column '{}' contains a disallowed function: {name}.",
                        table.columns[generated_idx].name
                    ),
                ));
                }
                if *distinct || crate::planner::is_aggregate_function(name) {
                    return Err(error(
                        crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                        "generated column expression cannot contain aggregate functions",
                    ));
                }
            }
            Expr::Unary { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::Binary { .. }
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::Case { .. }
            | Expr::Tuple(_)
            | Expr::GroupKeyRef { .. } => {}
            Expr::Literal(_) | Expr::BoundValue(_) => {}
            Expr::Wildcard
            | Expr::UserVariable(_)
            | Expr::SystemVariable { .. }
            | Expr::Placeholder(_)
            | Expr::Default(_)
            | Expr::DefaultQualified(_)
            | Expr::Grouping(_)
            | Expr::Subquery(_)
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
            | Expr::Exists { .. }
            | Expr::WindowFunction { .. } => {
                return Err(error(
                    crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                    "unsupported expression in generated column",
                ));
            }
        }
        Ok(true)
    })
}

fn collect_generated_expr_column(
    table: &TableSchema,
    generated_idx: usize,
    name: &str,
    out: &mut BTreeSet<String>,
) -> Result<(), QueryError> {
    let idx = table.column_index(name)?;
    if idx == generated_idx {
        return Err(error(
            crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
            format!(
                "generated column '{}' cannot refer to itself",
                table.columns[generated_idx].name
            ),
        ));
    }
    if table.columns[idx].generated_expr.is_some() && idx > generated_idx {
        return Err(error(
            crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
            "generated column expression cannot refer to a generated column defined later",
        ));
    }
    out.insert(table.columns[idx].name.clone());
    Ok(())
}

fn check_constraint_passes(
    table: &TableSchema,
    check: &CheckConstraintSchema,
    values: &[Value],
) -> Result<bool, QueryError> {
    let Some(program) = check.program.as_ref() else {
        return Err(error(
            crate::error::catalog::ER_CHECK_CONSTRAINT_FUNCTION_IS_NOT_ALLOWED,
            format!("CHECK constraint '{}' is not executable", check.name),
        ));
    };
    let value = program.eval(table, values)?;
    Ok(matches!(value, Value::Null) || crate::engine::expr::truthy(&value))
}

fn validate_existing_check_constraint_values(
    db: &Database,
    table: &TableSchema,
    check: &CheckConstraintSchema,
) -> Result<(), QueryError> {
    if !check.enforced {
        return Ok(());
    }
    for row in db.scan_rows(
        &table.qualified_name(),
        TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
    )? {
        if !check_constraint_passes(table, check, &row.values)? {
            return Err(check_constraint_error(&check.name));
        }
    }
    Ok(())
}

fn regenerate_check_constraint_names(checks: &mut [CheckConstraintSchema], table_name: &str) {
    for (idx, check) in checks.iter_mut().enumerate() {
        check.name = format!("{}_chk_{}", table_name, idx + 1);
    }
}

fn retarget_check_constraint_table_references(
    checks: &mut [CheckConstraintSchema],
    old_table_name: &str,
    new_table_name: &str,
) -> Result<(), QueryError> {
    for check in checks {
        let mut expr = crate::parser::parse_catalog_expr(&check.expr).map_err(|_| {
            error(
                crate::error::catalog::ER_CHECK_CONSTRAINT_FUNCTION_IS_NOT_ALLOWED,
                format!("CHECK constraint '{}' is not parseable", check.name),
            )
        })?;
        retarget_expr_table_qualifier(&mut expr, old_table_name, new_table_name);
        check.expr = encode_catalog_expr(&expr)?;
    }
    Ok(())
}

fn reject_column_referenced_by_check(
    table: &TableSchema,
    column_name: &str,
) -> Result<(), QueryError> {
    if table
        .check_constraints
        .iter()
        .any(|check| check.references_column(column_name))
    {
        return Err(error(
            crate::error::catalog::ER_DEPENDENT_BY_CHECK_CONSTRAINT,
            format!("cannot drop column '{column_name}' used by a CHECK constraint"),
        ));
    }
    Ok(())
}

fn reject_column_referenced_by_generated(
    table: &TableSchema,
    column_name: &str,
) -> Result<(), QueryError> {
    if table.columns.iter().any(|column| {
        !column.name.eq_ignore_ascii_case(column_name)
            && column
                .generated_column_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(column_name))
    }) {
        return Err(error(
            crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
            format!("cannot drop column '{column_name}' used by a generated column"),
        ));
    }
    Ok(())
}

/// P0-8: DROP COLUMN must reject any column referenced by the table's
/// PARTITION BY / SUBPARTITION BY clause. Previously we allowed the drop
/// and desynchronized `partitioning.columns` / `expr_sql` from the live
/// column list, breaking DML the moment the router tried to resolve the
/// column by name.
///
/// Checks (case-insensitive):
///  * `partition BY {RANGE,LIST,KEY} COLUMNS (...)` — `partitioning.columns`
///  * `partition BY {HASH,RANGE,LIST} (<expr>)` — column references in
///    `partitioning.expr_ast` (falling back to text search of `expr_sql`
///    when the AST couldn't be rebuilt).
///  * `subpartition BY KEY (...)` — `subpartitioning.columns`
///  * `subpartition BY HASH (<expr>)` — subpartitioning expression columns
pub(crate) fn reject_column_referenced_by_partitioning(
    table: &TableSchema,
    column_name: &str,
) -> Result<(), QueryError> {
    let Some(part) = table.partitioning.as_ref() else {
        return Ok(());
    };
    let referenced = part
        .columns
        .iter()
        .any(|c| c.eq_ignore_ascii_case(column_name))
        || partitioning_expr_references(part.expr_ast.as_ref(), &part.expr_sql, column_name)
        || part.subpartitioning.as_ref().is_some_and(|sub| {
            sub.columns
                .iter()
                .any(|c| c.eq_ignore_ascii_case(column_name))
                || partitioning_expr_references(
                    part.sub_expr_ast.as_ref(),
                    &sub.expr_sql,
                    column_name,
                )
        });
    if referenced {
        return Err(error(
            crate::error::catalog::ER_BAD_FIELD_ERROR,
            format!(
                "cannot drop or rename column '{column_name}' used by the partitioning function"
            ),
        ));
    }
    Ok(())
}

fn partitioning_expr_references(
    expr: Option<&crate::parser::Expr>,
    expr_sql: &str,
    column_name: &str,
) -> bool {
    if let Some(expr) = expr {
        return expr_references_column(expr, column_name);
    }
    if expr_sql.is_empty() {
        return false;
    }
    // Fallback for defensive parity when the AST is missing (e.g. legacy
    // catalog): case-insensitive identifier scan. Not perfect but safer
    // than allowing the drop and breaking DML.
    let needle = column_name.to_ascii_lowercase();
    let lower = expr_sql.to_ascii_lowercase();
    let mut haystack = lower.as_str();
    while let Some(idx) = haystack.find(&needle) {
        let ok_before = idx == 0
            || !haystack
                .as_bytes()
                .get(idx - 1)
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_');
        let end = idx + needle.len();
        let ok_after = end == haystack.len()
            || !haystack
                .as_bytes()
                .get(end)
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_');
        if ok_before && ok_after {
            return true;
        }
        haystack = &haystack[idx + 1..];
    }
    false
}

fn expr_references_column(expr: &Expr, column_name: &str) -> bool {
    crate::parser::visit::try_for_each_expr(expr, |expr| {
        let name = match expr {
            Expr::Identifier(name) => Some(name),
            Expr::CompoundIdentifier(parts) => parts.last(),
            Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
            | Expr::WindowFunction { .. }
            | Expr::Grouping(_)
            | Expr::GroupKeyRef { .. } => return Ok(false),
            _ => None,
        };
        if name.is_some_and(|name| name.eq_ignore_ascii_case(column_name)) {
            Err(())
        } else {
            Ok(true)
        }
    })
    .is_err()
}

fn rename_check_constraint_column_references(
    table: &mut TableSchema,
    old_name: &str,
    new_name: &str,
) -> Result<(), QueryError> {
    for check in &mut table.check_constraints {
        if !check.references_column(old_name) {
            continue;
        }
        let mut expr = crate::parser::parse_catalog_expr(&check.expr).map_err(|_| {
            error(
                crate::error::catalog::ER_CHECK_CONSTRAINT_FUNCTION_IS_NOT_ALLOWED,
                format!("CHECK constraint '{}' is not parseable", check.name),
            )
        })?;
        rename_expr_column(&mut expr, old_name, new_name);
        check.expr = encode_catalog_expr(&expr)?;
        for column in &mut check.column_names {
            if column.eq_ignore_ascii_case(old_name) {
                *column = new_name.to_string();
            }
        }
    }
    let checks = table
        .check_constraints
        .iter()
        .map(
            |check| -> Result<crate::parser::CheckConstraintDef, QueryError> {
                let expr = crate::parser::parse_catalog_expr(&check.expr).map_err(|_| {
                    error(
                        crate::error::catalog::ER_CHECK_CONSTRAINT_FUNCTION_IS_NOT_ALLOWED,
                        format!("CHECK constraint '{}' is not parseable", check.name),
                    )
                })?;
                Ok(crate::parser::CheckConstraintDef {
                    name: Some(check.name.clone()),
                    expr,
                    enforced: check.enforced,
                    column: None,
                })
            },
        )
        .collect::<Result<Vec<_>, _>>()?;
    table.check_constraints = build_check_constraints_for_table(table, checks, None, Some(1))?;
    Ok(())
}

/// P1-11: propagate a column rename into PARTITION BY / SUBPARTITION BY.
/// Rewrites `partitioning.columns` / `subpartitioning.columns` for
/// COLUMNS/KEY variants, and re-serializes `expr_sql` / `sub_expr_sql` for
/// HASH/RANGE(expr)/LIST(expr) variants. Finally re-runs
/// `rebuild_partition_expression_runtime` so the in-memory `expr_ast` and
/// compiled program reflect the new name.
pub(crate) fn rename_partition_column_references(
    table: &mut TableSchema,
    old_name: &str,
    new_name: &str,
) -> Result<(), QueryError> {
    let Some(part) = table.partitioning.as_mut() else {
        return Ok(());
    };
    let mut changed = false;
    for column in part.columns.iter_mut() {
        if column.eq_ignore_ascii_case(old_name) {
            *column = new_name.to_string();
            changed = true;
        }
    }
    if !part.expr_sql.is_empty() {
        let mut expr = crate::parser::parse_catalog_expr(&part.expr_sql).map_err(|_| {
            error(
                crate::error::catalog::ER_PARSE_ERROR,
                "PARTITION BY expression is not parseable during RENAME COLUMN",
            )
        })?;
        if expr_references_column(&expr, old_name) {
            rename_expr_column(&mut expr, old_name, new_name);
            part.expr_sql = encode_catalog_expr(&expr)?;
            changed = true;
        }
    }
    if let Some(sub) = part.subpartitioning.as_mut() {
        for column in sub.columns.iter_mut() {
            if column.eq_ignore_ascii_case(old_name) {
                *column = new_name.to_string();
                changed = true;
            }
        }
        if !sub.expr_sql.is_empty() {
            let mut expr = crate::parser::parse_catalog_expr(&sub.expr_sql).map_err(|_| {
                error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    "SUBPARTITION BY expression is not parseable during RENAME COLUMN",
                )
            })?;
            if expr_references_column(&expr, old_name) {
                rename_expr_column(&mut expr, old_name, new_name);
                sub.expr_sql = encode_catalog_expr(&expr)?;
                changed = true;
            }
        }
    }
    if changed {
        table.rebuild_partition_expression_runtime();
    }
    Ok(())
}

fn rename_column_expression_references(
    table: &mut TableSchema,
    old_name: &str,
    new_name: &str,
) -> Result<(), QueryError> {
    // DEFAULT expressions have the same schema scope as generated columns.
    // Rewrite identifiers through the AST so literals and function names stay intact.
    for column in &mut table.columns {
        if let Some(sql) = &mut column.default_expr {
            let mut expr = crate::parser::parse_catalog_expr(sql).map_err(|err| {
                error(crate::error::catalog::ER_PARSE_ERROR, err.to_string())
            })?;
            if expr_references_column(&expr, old_name) {
                rename_expr_column(&mut expr, old_name, new_name);
                *sql = encode_catalog_expr(&expr)?;
            }
        }
    }
    let mut changed = false;
    for column in &mut table.columns {
        let Some(expr_sql) = column.generated_expr.as_mut() else {
            continue;
        };
        let mut expr = crate::parser::parse_catalog_expr(expr_sql).map_err(|_| {
            error(
                crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                format!(
                    "generated column '{}' expression is not parseable",
                    column.name
                ),
            )
        })?;
        rename_expr_column(&mut expr, old_name, new_name);
        *expr_sql = encode_catalog_expr(&expr)?;
        for referenced in &mut column.generated_column_names {
            if referenced.eq_ignore_ascii_case(old_name) {
                *referenced = new_name.to_string();
            }
        }
        changed = true;
    }
    if changed {
        validate_and_prepare_generated_columns(table)?;
    }
    Ok(())
}

/// Rewrite schema identifiers without crossing into independently scoped queries.
fn rewrite_schema_identifiers(expr: &mut Expr, rewrite: impl FnMut(&mut Expr)) {
    use crate::parser::visit::{visit_expr_mut, AstVisitorMut};
    struct Rewrite<F>(F);
    impl<F: FnMut(&mut Expr)> AstVisitorMut for Rewrite<F> {
        fn enter_expr(&mut self, expr: &mut Expr) {
            match expr {
                Expr::Identifier(_) | Expr::CompoundIdentifier(_) => (self.0)(expr),
                Expr::QuantifiedComparison { left, right, .. } => {
                    visit_expr_mut(self, left);
                    if let QuantifiedComparisonRight::List(list) = right {
                        for item in list {
                            visit_expr_mut(self, item);
                        }
                    }
                }
                _ => {}
            }
        }
        fn walk_expr_children(&self, expr: &Expr) -> bool {
            !matches!(
                expr,
                Expr::Subquery(_)
                    | Expr::Exists { .. }
                    | Expr::InSubquery { .. }
                    | Expr::QuantifiedComparison { .. }
                    | Expr::WindowFunction { .. }
                    | Expr::Grouping(_)
            )
        }
    }
    visit_expr_mut(&mut Rewrite(rewrite), expr);
}

fn rename_expr_column(expr: &mut Expr, old_name: &str, new_name: &str) {
    rewrite_schema_identifiers(expr, |expr| {
        let name = match expr {
            Expr::Identifier(name) => Some(name),
            Expr::CompoundIdentifier(parts) => parts.last_mut(),
            _ => None,
        };
        if let Some(name) = name.filter(|name| name.eq_ignore_ascii_case(old_name)) {
            name.clear();
            name.push_str(new_name);
        }
    });
}

fn retarget_expr_table_qualifier(expr: &mut Expr, old_table_name: &str, new_table_name: &str) {
    rewrite_schema_identifiers(expr, |expr| {
        if let Expr::CompoundIdentifier(parts) = expr {
            if parts.len() == 2 && parts[0].eq_ignore_ascii_case(old_table_name) {
                parts[0].clear();
                parts[0].push_str(new_table_name);
            }
        }
    });
}

fn coerce_column_value(
    value: Value,
    column_name: &str,
    data_type: &DataTypeKind,
    unsigned: bool,
) -> Result<Value, QueryError> {
    coerce_value_with_unsigned(value, data_type, unsigned, column_name).map_err(|err| {
        if err.is(crate::error::catalog::ER_DATA_TOO_LONG) {
            err.with_message(format!("data too long for column '{column_name}'"))
        } else {
            err
        }
    })
}

/// Partition boundaries must never use INSERT's warning-and-clamp fallback.
/// Reuse the column codec, including valid temporal forms and binary padding,
/// but turn lossy/invalid conversions into DDL errors in every sql_mode.
fn coerce_partition_bound(value: Value, column: &ColumnSchema) -> Result<Value, QueryError> {
    struct Guard(bool);
    impl Drop for Guard {
        fn drop(&mut self) {
            PARTITION_BOUND_COERCION.with(|flag| flag.set(self.0));
        }
    }
    let previous = PARTITION_BOUND_COERCION.with(|flag| flag.replace(true));
    let _guard = Guard(previous);
    coerce_column_value(value, &column.name, &column.data_type, column.unsigned)
}

/// Coerce a stored-program parameter, local variable, OUT value or return
/// value through the same conversion implementation used by DML columns.
/// Keeping this entry point here prevents routines from growing a second set
/// of decimal, temporal, ENUM/SET and strict-mode semantics.
pub(crate) fn coerce_routine_typed_value(
    value: Value,
    name: &str,
    data_type: &DataTypeKind,
    unsigned: bool,
) -> Result<Value, QueryError> {
    coerce_column_value(value, name, data_type, unsigned)
}

fn coerce_column_value_for_schema(
    mut value: Value,
    column: &ColumnSchema,
) -> Result<Value, QueryError> {
    // Already typed text within the byte bound is also within the character
    // bound. Keep charset validation, and canonicalize CHAR padding in place.
    // Oversized/multibyte assignments still use the diagnostic conversion path.
    if let Value::String(text) = &mut value {
        let fits = match column.data_type {
            DataTypeKind::Char { len } if text.len() <= len.unwrap_or(1) => {
                let end = crate::util::pad_space::trim_trailing_spaces_len(text.as_bytes());
                text.truncate(end);
                true
            }
            DataTypeKind::VarChar { len } => len.is_none_or(|len| text.len() <= len),
            _ => false,
        };
        if fits {
            validate_column_charset_value(column, &value)?;
            return Ok(value);
        }
    }
    // Typed integers inside the declared domain require no conversion or
    // diagnostics. Other representations and out-of-range values retain the
    // canonical strict/IGNORE conversion path below.
    if integer_value_fits_column(&value, column) {
        return Ok(value);
    }
    if matches!(
        (&value, &column.data_type),
        (Value::Bytes(bytes), DataTypeKind::Enum { .. }) if bytes.is_empty()
    ) {
        let already_stored_sentinel = ENUM_FIELD_COPY_COERCION_DEPTH.with(|depth| depth.get() != 0);
        let schema_default_sentinel = column
            .default
            .as_ref()
            .is_some_and(|default| matches!(default, Value::Bytes(bytes) if bytes.is_empty()));
        if already_stored_sentinel || schema_default_sentinel {
            return Ok(enum_zero_value());
        }
    }
    let value = if matches!(
        column.data_type,
        DataTypeKind::Binary { .. } | DataTypeKind::VarBinary { .. } | DataTypeKind::Blob { .. }
    ) {
        crate::engine::expr::encode_binary_assignment_value(
            value,
            crate::engine::expr::connection_charset(),
        )?
    } else {
        value
    };
    let value = decode_binary_assignment_for_character_column(value, column)?;
    if let Some(collation) = column.collation.as_deref().filter(|c| !c.is_empty()) {
        if matches!(
            column.data_type,
            DataTypeKind::Enum { .. } | DataTypeKind::Set { .. }
        ) {
            let value = coerce_enum_set_with_collation(value, column, collation)?;
            validate_column_charset_value(column, &value)?;
            return Ok(value);
        }
    }
    let value = coerce_column_value(value, &column.name, &column.data_type, column.unsigned)?;
    // Collation-aware ENUM/SET re-canonicalisation: the inner
    // coerce_value_with_unsigned lacks the column's collation and
    // falls back to ASCII case folding. Redo the lookup here with the
    // real collation so `utf8mb4_0900_ai_ci` accepts `'É' vs 'e'` and
    // `utf8mb4_bin` stays strict.
    let value = if matches!(
        column.data_type,
        DataTypeKind::Enum { .. } | DataTypeKind::Set { .. }
    ) {
        enum_set_recanonicalize_with_collation(column, value)?
    } else {
        value
    };
    validate_column_charset_value(column, &value)?;
    Ok(value)
}

#[inline]
fn integer_value_fits_column(value: &Value, column: &ColumnSchema) -> bool {
    if !matches!(value, Value::Int(_) | Value::UInt(_)) {
        return false;
    }
    let bits = match column.data_type {
        DataTypeKind::TinyInt | DataTypeKind::Bool => 8,
        DataTypeKind::SmallInt => 16,
        DataTypeKind::MediumInt => 24,
        DataTypeKind::Int => 32,
        DataTypeKind::BigInt => 64,
        _ => return false,
    };
    match (value, column.unsigned) {
        (Value::Int(number), false) => {
            let high = number >> (bits - 1);
            high == 0 || high == -1
        }
        (Value::UInt(number), true) => *number <= u64::MAX >> (64 - bits),
        _ => false,
    }
}

thread_local! {
    static ENUM_FIELD_COPY_COERCION_DEPTH: std::cell::Cell<u16> =
        const { std::cell::Cell::new(0) };
}

pub(crate) fn with_enum_field_copy_coercion<T>(f: impl FnOnce() -> T) -> T {
    struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            ENUM_FIELD_COPY_COERCION_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
        }
    }
    ENUM_FIELD_COPY_COERCION_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
    let guard = Guard;
    let result = f();
    drop(guard);
    result
}

/// Interpret a binary string assigned to a character column in the target
/// column's character set.  `Value::Bytes` is used by hexadecimal/bit
/// literals, binary-protocol parameters and functions such as AES_ENCRYPT;
/// decoding it as UTF-8 lossy text would both corrupt valid latin1 bytes and
/// silently replace malformed utf8mb4 input.
fn decode_binary_assignment_for_character_column(
    value: Value,
    column: &ColumnSchema,
) -> Result<Value, QueryError> {
    if !matches!(
        column.data_type,
        DataTypeKind::Char { .. }
            | DataTypeKind::VarChar { .. }
            | DataTypeKind::Text { .. }
            | DataTypeKind::Enum { .. }
            | DataTypeKind::Set { .. }
    ) {
        return Ok(value);
    }
    let Value::Bytes(bytes) = value else {
        return Ok(value);
    };
    let charset = column
        .charset
        .as_deref()
        .filter(|charset| !charset.is_empty())
        .or_else(|| {
            column
                .collation
                .as_deref()
                .filter(|collation| !collation.is_empty())
                .and_then(|collation| {
                    crate::charset::resolve_collation(collation)
                        .ok()
                        .map(|info| info.charset)
                })
        })
        .unwrap_or(crate::charset::DEFAULT_CHARSET);
    crate::charset::decode_bytes(charset, &bytes).map(Value::String)
}

fn coerce_enum_set_with_collation(
    value: Value,
    column: &ColumnSchema,
    collation: &str,
) -> Result<Value, QueryError> {
    // Preserve the common coercion-kernel NULL invariant. This helper is a
    // collation-aware fast path and must not reinterpret SQL NULL as the
    // display string "NULL".
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    match &column.data_type {
        DataTypeKind::Enum { values } => {
            // Row-codec identity for ENUM ordinal zero. A zero-length byte
            // value renders as the same empty SQL text but remains distinct
            // from a legitimately declared empty member (`SmallString("")`)
            // in GROUP BY/DISTINCT hash keys and field-to-field copies.
            if matches!(&value, Value::Bytes(bytes) if bytes.is_empty()) {
                return Ok(enum_zero_value());
            }
            let ordinal = match &value {
                Value::UInt(value) => Some(*value as i128),
                Value::Int(value) => Some(*value as i128),
                _ => None,
            };
            if let Some(ordinal) = ordinal {
                if ordinal == 0 {
                    // Numeric zero selects ENUM's internal error/empty
                    // sentinel, but it is still an invalid member
                    // conversion. MySQL stores the sentinel with warning
                    // 1265 in non-strict mode and rejects it in strict mode.
                    // A field-to-field copy of an already stored sentinel
                    // uses Value::Bytes(empty) above and must not warn again.
                    return enum_set_truncation(enum_zero_value(), "ENUM", &column.name);
                }
                if ordinal >= 1 && (ordinal as usize) <= values.len() {
                    return Ok(enum_member_value(values[(ordinal - 1) as usize].clone()));
                }
                return enum_set_truncation(enum_zero_value(), "ENUM", &column.name);
            }
            let text = match value {
                Value::String(value) | Value::Decimal(value) => value,
                Value::Bytes(value) => String::from_utf8_lossy(&value).to_string(),
                other => value_to_coercion_string(&other)?,
            };
            if let Some(member) = enum_assignment_member(&text, values, Some(collation)) {
                Ok(enum_member_value(member))
            } else {
                enum_set_truncation(enum_zero_value(), "ENUM", &column.name)
            }
        }
        DataTypeKind::Set { values } => {
            let numeric = match &value {
                Value::UInt(value) => Some(*value),
                Value::Int(value) if *value >= 0 => Some(*value as u64),
                Value::Int(_) => {
                    return enum_set_truncation(Value::String(String::new()), "SET", &column.name);
                }
                Value::Float(value) => {
                    let value = f64::from(*value);
                    if value.is_finite() && value >= 0.0 && value <= u64::MAX as f64 {
                        let mask = value.trunc() as u64;
                        if value.fract() == 0.0 {
                            Some(mask)
                        } else {
                            return enum_set_truncation(
                                Value::String(set_bitmask_to_csv(mask, values)),
                                "SET",
                                &column.name,
                            );
                        }
                    } else {
                        let mask = if value.is_sign_negative() {
                            0
                        } else {
                            u64::MAX
                        };
                        return enum_set_truncation(
                            Value::String(set_bitmask_to_csv(mask, values)),
                            "SET",
                            &column.name,
                        );
                    }
                }
                Value::Double(value) => {
                    if value.is_finite() && *value >= 0.0 && *value <= u64::MAX as f64 {
                        let mask = value.trunc() as u64;
                        if value.fract() == 0.0 {
                            Some(mask)
                        } else {
                            return enum_set_truncation(
                                Value::String(set_bitmask_to_csv(mask, values)),
                                "SET",
                                &column.name,
                            );
                        }
                    } else {
                        let mask = if value.is_sign_negative() {
                            0
                        } else {
                            u64::MAX
                        };
                        return enum_set_truncation(
                            Value::String(set_bitmask_to_csv(mask, values)),
                            "SET",
                            &column.name,
                        );
                    }
                }
                _ => None,
            };
            if let Some(mask) = numeric {
                return Ok(Value::String(set_bitmask_to_csv(mask, values)));
            }
            let text = match value {
                Value::String(value) | Value::Decimal(value) => value,
                Value::Bytes(value) => String::from_utf8_lossy(&value).to_string(),
                other => value_to_coercion_string(&other)?,
            };
            if !text.contains(',') {
                let trimmed = text.trim_end_matches(' ');
                if let Some(member) = enum_lookup_member(trimmed, values, Some(collation)) {
                    return Ok(Value::String(member));
                }
                if let Ok(mask) = trimmed.parse::<u64>() {
                    return Ok(Value::String(set_bitmask_to_csv(mask, values)));
                }
            }
            let mut result = Vec::new();
            let mut truncated = false;
            for token in text.split(',') {
                let trimmed = token.trim_end_matches(' ');
                if trimmed.is_empty() {
                    continue;
                }
                let Some(member) = enum_lookup_member(trimmed, values, Some(collation)) else {
                    truncated = true;
                    continue;
                };
                if !result.iter().any(|value| value == &member) {
                    result.push(member);
                }
            }
            result.sort_by_key(|member| {
                values
                    .iter()
                    .position(|value| value == member)
                    .unwrap_or(usize::MAX)
            });
            let value = Value::String(result.join(","));
            if truncated {
                enum_set_truncation(value, "SET", &column.name)
            } else {
                Ok(value)
            }
        }
        _ => unreachable!("caller restricts this helper to ENUM/SET"),
    }
}

fn enum_set_truncation(fallback: Value, kind: &str, column: &str) -> Result<Value, QueryError> {
    let message = format!("Data truncated for column '{column}' at row 1");
    if coercion_strict() {
        Err(error(
            crate::error::catalog::WARN_DATA_TRUNCATED,
            format!("data truncated for {kind} column '{column}'"),
        ))
    } else {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::WARN_DATA_TRUNCATED,
            message,
        );
        Ok(fallback)
    }
}

fn enum_member_value(member: String) -> Value {
    if member.is_empty() {
        Value::SmallString(crate::types::SmallStringBuf::from_slice(b""))
    } else {
        Value::String(member)
    }
}

fn enum_zero_value() -> Value {
    Value::Bytes(Vec::new())
}

fn enum_set_recanonicalize_with_collation(
    column: &ColumnSchema,
    value: Value,
) -> Result<Value, QueryError> {
    let collation = column.collation.as_deref().filter(|c| !c.is_empty());
    // Nothing to do without an explicit collation (ASCII path already
    // ran); avoids penalising the common utf8mb4-default case.
    let Some(collation) = collation else {
        return Ok(value);
    };
    match &column.data_type {
        DataTypeKind::Enum { values } => match &value {
            Value::String(s) if !s.is_empty() => {
                if values.iter().any(|m| m == s) {
                    return Ok(value);
                }
                if let Some(matched) = enum_lookup_member(s, values, Some(collation)) {
                    return Ok(enum_member_value(matched));
                }
                Err(error(
                    crate::error::catalog::WARN_DATA_TRUNCATED,
                    format!("data truncated for ENUM column '{}'", column.name),
                ))
            }
            _ => Ok(value),
        },
        DataTypeKind::Set { values } => match &value {
            Value::String(csv) if !csv.is_empty() => {
                let mut result: Vec<String> = Vec::new();
                for token in csv.split(',') {
                    if token.is_empty() {
                        continue;
                    }
                    if values.iter().any(|m| m == token) {
                        if !result.iter().any(|v| v == token) {
                            result.push(token.to_string());
                        }
                        continue;
                    }
                    if let Some(matched) = enum_lookup_member(token, values, Some(collation)) {
                        if !result.iter().any(|v| v == &matched) {
                            result.push(matched);
                        }
                    } else {
                        return Err(error(
                            crate::error::catalog::WARN_DATA_TRUNCATED,
                            format!("data truncated for SET column '{}'", column.name),
                        ));
                    }
                }
                result.sort_by_key(|m| values.iter().position(|v| v == m).unwrap_or(usize::MAX));
                Ok(Value::String(result.join(",")))
            }
            _ => Ok(value),
        },
        _ => Ok(value),
    }
}

fn validate_column_charset_value(column: &ColumnSchema, value: &Value) -> Result<(), QueryError> {
    let Some(charset) = column.charset.as_deref() else {
        return Ok(());
    };
    match value {
        Value::String(text) | Value::Decimal(text) => {
            crate::charset::validate_text_for_charset(charset, text)
        }
        _ => Ok(()),
    }
}

/// MySQL two-digit-year pivot for compact date forms: `00..=69` → `2000..2069`,
/// `70..=99` → `1970..1999`. Non-numeric input falls back to year 2000.
fn pivot_two_digit_year(yy: &str) -> i32 {
    match yy.parse::<i32>() {
        Ok(y) if y <= 69 => 2000 + y,
        Ok(y) => 1900 + y,
        Err(_) => 2000,
    }
}

fn coerce_to_date_string(value: Value) -> Result<String, QueryError> {
    let s = match value {
        Value::String(v) | Value::Decimal(v) => v,
        // A typed TIME converted to DATE uses the current statement date;
        // this is observable with `SET timestamp=...` and is distinct from a
        // character string such as '23:59:59'.
        Value::Time(_) => crate::engine::expr::session_now()
            .date()
            .format("%Y-%m-%d")
            .to_string(),
        Value::Int(v) => {
            let s = v.to_string();
            if s.len() == 8 {
                return coerce_to_date_string(Value::String(format!(
                    "{}-{}-{}",
                    &s[0..4],
                    &s[4..6],
                    &s[6..8]
                )));
            }
            // 6-digit `YYMMDD` with the MySQL two-digit-year pivot
            // (`200101` → 2020-01-01, `700101` → 1970-01-01).
            if s.len() == 6 {
                return coerce_to_date_string(Value::String(format!(
                    "{:04}-{}-{}",
                    pivot_two_digit_year(&s[0..2]),
                    &s[2..4],
                    &s[4..6]
                )));
            }
            s
        }
        Value::Bytes(v) => String::from_utf8_lossy(&v).to_string(),
        other => value_to_coercion_string(&other)?,
    };
    let trimmed = s.trim();
    if is_numeric_zero_temporal_literal(trimmed) {
        return Ok("0000-00-00".to_string());
    }
    // MySQL's zero date never survives a chrono parse (month 0); pass it
    // through so `parse_date_to_days` maps it to the zero sentinel and
    // the NO_ZERO_DATE policy can decide.
    if trimmed == "0000-00-00" || trimmed.starts_with("0000-00-00 ") {
        return Ok("0000-00-00".to_string());
    }
    // Canonical text is validated and converted to binary days immediately
    // afterwards by `parse_date_to_days`, whose YYYY-MM-DD path is a small
    // allocation-free parser. Do not run the same value through chrono here
    // merely to format it back to identical text: wide fact-table INSERTs
    // commonly carry several DATE columns, making that parse+format roundtrip
    // tens of millions of redundant operations per load. Invalid canonical-
    // shaped dates deliberately pass through too; the downstream parser still
    // routes them through the exact sql_mode-aware error/warning policy.
    let bytes = trimmed.as_bytes();
    if bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[8..].iter().all(u8::is_ascii_digit)
    {
        return if trimmed.len() == s.len() {
            Ok(s)
        } else {
            Ok(trimmed.to_string())
        };
    }
    if let Some(parts) = crate::util::temporal::parse_delimited_datetime_components(trimmed) {
        return Ok(format!(
            "{:04}-{:02}-{:02}",
            parts.year, parts.month, parts.day
        ));
    }
    // Normalize when chrono accepts the form; otherwise hand the raw
    // string to `parse_date_to_days`, whose failure routes through the
    // sql_mode-aware invalid-date policy (strict error vs warning+zero)
    // instead of failing fast here.
    Ok(NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|_| trimmed.to_string()))
}

/// Look up a candidate ENUM/SET token against the declared member list
/// using the column's collation (so `utf8mb4_0900_ai_ci` matches
/// `'É' = 'e'`, and `utf8mb4_bin` stays strict). Returns the matched
/// member string in its declared spelling, or None if unmatched.
fn enum_assignment_member(
    input: &str,
    members: &[String],
    collation: Option<&str>,
) -> Option<String> {
    enum_lookup_member(input, members, collation).or_else(|| {
        let ordinal = input.trim().parse::<usize>().ok()?;
        ordinal
            .checked_sub(1)
            .and_then(|index| members.get(index))
            .cloned()
    })
}

pub(crate) fn enum_lookup_member(
    input: &str,
    members: &[String],
    collation: Option<&str>,
) -> Option<String> {
    if let Some(coll) = collation.filter(|c| !c.is_empty()) {
        for m in members {
            if crate::charset::compare_strings(coll, input, m) == std::cmp::Ordering::Equal {
                return Some(m.clone());
            }
        }
        None
    } else {
        let input = crate::util::pad_space::pad_space_trim_end_str(input);
        for m in members {
            if crate::util::pad_space::pad_space_trim_end_str(m).eq_ignore_ascii_case(input) {
                return Some(m.clone());
            }
        }
        None
    }
}

/// Render a SET bitmask as a canonical CSV string in declared-member
/// order (single source of truth used by INSERT coercion, ALTER COPY
/// lossy fallback, and binlog reverse decoding).
pub(crate) fn set_bitmask_to_csv(mask: u64, members: &[String]) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for (idx, m) in members.iter().enumerate() {
        if idx >= 64 {
            break;
        }
        if mask & (1u64 << idx) != 0 {
            parts.push(m.as_str());
        }
    }
    parts.join(",")
}

/// DDL-time member-list validation. MySQL 8.0 caps ENUM at 65 535 unique
/// members and SET at 64 unique members, compares duplicates under the
/// column collation, forbids commas inside SET members, and rejects empty
/// member lists. Duplicate members are errors in strict mode and notes in
/// non-strict mode.
pub(crate) fn validate_enum_set_members(
    column: &str,
    values: &[String],
    is_set: bool,
    collation: Option<&str>,
) -> Result<(), QueryError> {
    if values.is_empty() {
        return Err(error(
            crate::error::catalog::ER_DUPLICATED_VALUE_IN_TYPE,
            format!(
                "Column '{column}' has no values for {} type",
                if is_set { "SET" } else { "ENUM" }
            ),
        ));
    }
    let key_for = |value: &str| {
        if let Some(collation) = collation.filter(|value| !value.is_empty()) {
            crate::charset::collation_key(collation, value)
        } else {
            crate::util::pad_space::pad_space_trim_end_str(value)
                .to_ascii_lowercase()
                .into_bytes()
        }
    };
    // Key once per member: ENUM permits 65 535 entries, so a pairwise
    // duplicate scan would turn legitimate DDL into quadratic work.
    let mut seen: HashMap<Vec<u8>, usize> = HashMap::with_capacity(values.len());
    let mut duplicates = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let symbols = value.chars().count();
        if symbols > 255 {
            return Err(error(
                crate::error::catalog::ER_TOO_LONG_SET_ENUM_VALUE,
                format!(
                    "String '{}' is too long for {} (should be no longer than 255)",
                    value,
                    if is_set { "SET" } else { "ENUM" }
                ),
            ));
        }
        if seen.insert(key_for(value), index).is_some() {
            duplicates.push(index);
        }
    }
    let limit = if is_set { 64 } else { 65_535 };
    if seen.len() > limit {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_SET,
            format!(
                "Too many {} members for column '{column}' ({} unique, max {limit})",
                if is_set { "SET" } else { "ENUM" },
                seen.len()
            ),
        ));
    }
    if is_set {
        for v in values {
            if v.contains(',') {
                return Err(error(crate::error::catalog::ER_ILLEGAL_VALUE_FOR_TYPE,
                    format!(
                        "Illegal SET member '{v}' for column '{column}' — SET members cannot contain a comma"
                    ),
                ));
            }
        }
    }
    for index in duplicates {
        let message = format!(
            "Column '{column}' has duplicated value '{}' in {} type",
            values[index],
            if is_set { "SET" } else { "ENUM" }
        );
        if coercion_strict() {
            return Err(error(
                crate::error::catalog::ER_DUPLICATED_VALUE_IN_TYPE,
                message,
            ));
        }
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Note,
            crate::error::catalog::ER_DUPLICATED_VALUE_IN_TYPE,
            message,
        );
    }
    Ok(())
}

/// Given a literal DEFAULT value already parsed for an ENUM / SET
/// column, check that it names a member (or is a legal numeric ordinal
/// for ENUM). Returns Some(err) on mismatch — the DDL should abort
/// with ER_INVALID_DEFAULT(1067) so the bad table is never created.
fn enum_set_default_error(
    column: &str,
    data_type: &DataTypeKind,
    literal: &Value,
    collation: Option<&str>,
) -> Option<QueryError> {
    let invalid = || {
        Some(error(
            crate::error::catalog::ER_INVALID_DEFAULT,
            format!("Invalid default value for '{column}'"),
        ))
    };
    match data_type {
        DataTypeKind::Enum { values } => {
            match literal {
                Value::String(s) | Value::Decimal(s) => {
                    if s.is_empty() {
                        return None; // '' is the empty-index sentinel
                    }
                    if enum_lookup_member(s, values, collation).is_some() {
                        None
                    } else {
                        invalid()
                    }
                }
                Value::Int(v) => {
                    let ord = *v;
                    if ord == 0 || (ord >= 1 && (ord as usize) <= values.len()) {
                        None
                    } else {
                        invalid()
                    }
                }
                Value::UInt(v) => {
                    if *v == 0 || (*v >= 1 && (*v as usize) <= values.len()) {
                        None
                    } else {
                        invalid()
                    }
                }
                _ => invalid(),
            }
        }
        DataTypeKind::Set { values } => match literal {
            Value::String(s) | Value::Decimal(s) => {
                if s.is_empty() {
                    return None;
                }
                for token in s.split(',') {
                    let trimmed = token.trim_end_matches(' ');
                    if trimmed.is_empty() {
                        continue;
                    }
                    if enum_lookup_member(trimmed, values, collation).is_none() {
                        return invalid();
                    }
                }
                None
            }
            Value::Int(v) if *v >= 0 => None,
            Value::UInt(_) => None,
            _ => invalid(),
        },
        _ => None,
    }
}

fn value_to_coercion_string(value: &Value) -> Result<String, QueryError> {
    Ok(match value {
        Value::Json(bytes) => crate::engine::json::unquote_text(bytes),
        Value::Timestamp(micros) => {
            crate::engine::expr::session_utc_timestamp_to_local_datetime_micros(*micros)
                .map(Value::DateTime)?
                .to_display_string()
        }
        _ => value_to_string(value),
    })
}

fn coerce_to_datetime_string(value: Value) -> Result<String, QueryError> {
    let s = match value {
        Value::String(v) | Value::Decimal(v) => v,
        Value::Int(v) => {
            let s = v.to_string();
            if s.len() == 14 {
                return coerce_to_datetime_string(Value::String(format!(
                    "{}-{}-{} {}:{}:{}",
                    &s[0..4],
                    &s[4..6],
                    &s[6..8],
                    &s[8..10],
                    &s[10..12],
                    &s[12..14]
                )));
            }
            if s.len() == 8 {
                return coerce_to_datetime_string(Value::String(format!(
                    "{}-{}-{} 00:00:00",
                    &s[0..4],
                    &s[4..6],
                    &s[6..8]
                )));
            }
            // 12-digit `YYMMDDHHMMSS` and 6-digit `YYMMDD` with the MySQL
            // two-digit-year pivot.
            if s.len() == 12 {
                return coerce_to_datetime_string(Value::String(format!(
                    "{:04}-{}-{} {}:{}:{}",
                    pivot_two_digit_year(&s[0..2]),
                    &s[2..4],
                    &s[4..6],
                    &s[6..8],
                    &s[8..10],
                    &s[10..12]
                )));
            }
            if s.len() == 6 {
                return coerce_to_datetime_string(Value::String(format!(
                    "{:04}-{}-{} 00:00:00",
                    pivot_two_digit_year(&s[0..2]),
                    &s[2..4],
                    &s[4..6]
                )));
            }
            s
        }
        Value::Bytes(v) => String::from_utf8_lossy(&v).to_string(),
        other => value_to_coercion_string(&other)?,
    };
    let trimmed = s.trim();
    if is_numeric_zero_temporal_literal(trimmed) {
        return Ok("0000-00-00 00:00:00".to_string());
    }
    // Canonical, valid input already has the representation consumed by the
    // storage parser. Avoid a chrono parse -> format -> parse round trip.
    // Keep unusual forms (including leap seconds) on the normalization path.
    // Preserve excess fraction digits until assignment precision is known.
    if datetime_needs_no_normalization(trimmed.as_bytes()) {
        return Ok(s);
    }
    if let Some(parts) = crate::util::temporal::parse_delimited_datetime_components(trimmed) {
        if parts.year == 0
            && parts.month == 0
            && parts.day == 0
            && parts.hour == 0
            && parts.minute == 0
            && parts.second == 0
            && parts.micros == 0
        {
            return Ok("0000-00-00 00:00:00".to_string());
        }
        let fraction = trimmed
            .rsplit_once('.')
            .map(|(_, fraction)| fraction)
            .filter(|fraction| {
                fraction.len() > 6 && fraction.bytes().all(|byte| byte.is_ascii_digit())
            });
        if let Some(fraction) = fraction {
            return Ok(format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{fraction}",
                parts.year, parts.month, parts.day, parts.hour, parts.minute, parts.second,
            ));
        }
        return Ok(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            parts.year,
            parts.month,
            parts.day,
            parts.hour,
            parts.minute,
            parts.second,
            parts.micros
        ));
    }
    // Preserve sub-second precision when present so the caller's fsp
    // truncation step has microseconds to operate on. Formatting without
    // `%.f` (the prior code) silently dropped the fractional component,
    // which made `DATETIME(3)` look like `DATETIME(0)` on the read side.
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.format("%Y-%m-%d %H:%M:%S").to_string());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S") {
        return Ok(dt.format("%Y-%m-%d %H:%M:%S").to_string());
    }
    if let Ok(d) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return Ok(format!("{} 00:00:00", d.format("%Y-%m-%d")));
    }
    // Zero datetime (chrono can't represent it) and anything else
    // unparseable pass through raw: `parse_datetime_to_micros` maps the
    // zero form to the sentinel and fails on garbage, which then routes
    // through the sql_mode-aware invalid-date policy.
    Ok(trimmed.to_string())
}

fn datetime_needs_no_normalization(bytes: &[u8]) -> bool {
    if !(bytes.len() == 19 || bytes.len() >= 21)
        || !matches!(bytes[10], b' ' | b'T')
        || bytes[13] != b':'
        || bytes[16] != b':'
        || (bytes.len() > 19 && (bytes[19] != b'.' || !bytes[20..].iter().all(u8::is_ascii_digit)))
    {
        return false;
    }
    let (Some((year, month, day)), Some(hour), Some(minute), Some(second)) = (
        parse_ymd_fast(&bytes[..10]),
        parse_2_digit(&bytes[11..13]),
        parse_2_digit(&bytes[14..16]),
        parse_2_digit(&bytes[17..19]),
    ) else {
        return false;
    };
    hour < 24 && minute < 60 && second < 60 && days_from_civil(year, month, day).is_some()
}

#[cfg(test)]
mod datetime_normalization_tests {
    use super::*;

    #[test]
    fn canonical_datetime_coercion_matches_chrono_normalization() {
        for year in [0, 1, 1900, 1969, 1970, 2000, 2024, 2038, 9999] {
            for month in 1..=12 {
                for day in [1, 28, 29, 30, 31] {
                    for separator in [' ', 'T'] {
                        for fraction in
                            ["", ".0", ".1", ".123", ".999999", ".1234567", ".999999999"]
                        {
                            let input = format!(
                                "{year:04}-{month:02}-{day:02}{separator}23:59:59{fraction}"
                            );
                            let format = if separator == 'T' {
                                "%Y-%m-%dT%H:%M:%S%.f"
                            } else {
                                "%Y-%m-%d %H:%M:%S%.f"
                            };
                            let expected = chrono::NaiveDateTime::parse_from_str(&input, format);
                            assert_eq!(
                                datetime_needs_no_normalization(input.as_bytes()),
                                expected.is_ok(),
                                "{input}"
                            );
                            if let Ok(expected) = expected {
                                let output =
                                    coerce_to_datetime_string(Value::String(input.clone()))
                                        .unwrap();
                                assert_eq!(
                                    parse_datetime_to_micros(&output).unwrap(),
                                    expected.and_utc().timestamp_micros(),
                                    "{input}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn unusual_datetime_forms_keep_normalization_fallback() {
        for input in [
            "",
            "0",
            "0000-00-00 00:00:00",
            "2024-02-30 12:00:00",
            "2024-01-01 24:00:00",
            "2024-01-01 12:60:00",
            "2024-01-01 23:59:60",
            "2024-01-01 12:00:00.",
            "2024-01-01 12:00:00+08:00",
            "2024-1-1 1:2:3",
            "20240101120000",
            "2024-01-01 12:00:00.中文",
        ] {
            assert!(
                !datetime_needs_no_normalization(input.as_bytes()),
                "{input}"
            );
        }
    }
}

/// MySQL accepts punctuation other than `:` between time fields for legacy
/// compatibility, but emits ER_WARN_DEPRECATED_DATETIME_DELIMITER once for
/// each converted value. Keep this diagnostic at the materialization
/// boundary so INSERT/UPDATE, prepared values, and every execution strategy
/// share it without changing the permissive parser.
fn warn_deprecated_datetime_delimiter(value: &Value) {
    let text = match value {
        Value::String(value) | Value::Decimal(value) => value.as_str(),
        Value::SmallString(value) => {
            let Ok(text) = std::str::from_utf8(value.as_slice()) else {
                return;
            };
            text
        }
        Value::Bytes(value) => {
            let Ok(text) = std::str::from_utf8(value) else {
                return;
            };
            text
        }
        _ => return,
    };
    let bytes = text.trim().as_bytes();
    if bytes.len() < 19
        || !matches!(bytes[10], b' ' | b'T')
        || !bytes[11..13].iter().all(u8::is_ascii_digit)
        || !bytes[14..16].iter().all(u8::is_ascii_digit)
        || !bytes[17..19].iter().all(u8::is_ascii_digit)
    {
        return;
    }
    let deprecated = (bytes[13].is_ascii_punctuation() && bytes[13] != b':')
        || (bytes[16].is_ascii_punctuation() && bytes[16] != b':');
    if deprecated {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_DEPRECATED_DATETIME_DELIMITER,
            format!(
                "Delimiter in position 14 or 17 in datetime value '{}' is deprecated",
                text.trim()
            ),
        );
    }
}

#[inline]
fn is_numeric_zero_temporal_literal(text: &str) -> bool {
    let text = text
        .strip_prefix('+')
        .or_else(|| text.strip_prefix('-'))
        .unwrap_or(text);
    !text.is_empty()
        && text.bytes().all(|byte| matches!(byte, b'0' | b'.'))
        && text.bytes().any(|byte| byte == b'0')
        && text.bytes().filter(|byte| *byte == b'.').count() <= 1
}

/// Longest MySQL decimal prefix, excluding surrounding ASCII whitespace.
/// The boolean is true only when the entire trimmed input was consumed.
fn decimal_numeric_prefix(raw: &str) -> (Option<&str>, bool) {
    let trimmed = raw.trim_matches(|ch: char| ch.is_ascii_whitespace());
    let bytes = trimmed.as_bytes();
    let mut index = usize::from(matches!(bytes.first(), Some(b'+') | Some(b'-')));
    let mut digits = 0usize;
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
        digits += 1;
    }
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return (None, false);
    }
    if matches!(bytes.get(index), Some(b'e') | Some(b'E')) {
        let mut exponent_end = index + 1;
        if matches!(bytes.get(exponent_end), Some(b'+') | Some(b'-')) {
            exponent_end += 1;
        }
        let exponent_start = exponent_end;
        while exponent_end < bytes.len() && bytes[exponent_end].is_ascii_digit() {
            exponent_end += 1;
        }
        if exponent_end > exponent_start {
            index = exponent_end;
        }
    }
    (Some(&trimmed[..index]), index == bytes.len())
}

fn format_decimal(
    raw: &str,
    precision: Option<u8>,
    scale: Option<u8>,
    column: &str,
) -> Result<String, QueryError> {
    let target_scale = scale.unwrap_or(0);
    let sc = target_scale as usize;
    // Exact parse + rescale to `target_scale` using u128 magnitudes — never an
    // f64. The previous f64 round-trip silently truncated any DECIMAL literal
    // with more than ~15-17 significant digits (e.g. 123456789012345678.99
    // became 123456789012345680.00). Excess fractional digits are truncated,
    // matching the DecimalCompact path's existing MySQL-style semantics.
    let parsed = parse_decimal_string_to_magnitude(raw, target_scale);
    if let Err(original) = &parsed {
        // Keep the u128 parser as the normal path. Only values wider than 38
        // digits enter the allocation-backed DECIMAL(65) kernel; malformed
        // text still returns the original 1366 error.
        let Some(formatted) = crate::util::decimal::rescale_decimal_big_str(raw, target_scale)
        else {
            if !coercion_strict() {
                let (prefix, clean) = decimal_numeric_prefix(raw);
                if !clean {
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Warning,
                        crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                        format!("Incorrect decimal value: '{raw}' for column '{column}' at row 1"),
                    );
                    return match prefix {
                        Some(prefix) => format_decimal(prefix, precision, scale, column),
                        None => Ok(if target_scale == 0 {
                            "0".to_string()
                        } else {
                            format!("0.{}", "0".repeat(target_scale as usize))
                        }),
                    };
                }
                if let Some(prefix) = prefix {
                    // A complete scientific form reaches here only when its
                    // exponent is outside the representable DECIMAL range.
                    // Negative exponents round to zero at every legal scale;
                    // positive exponents saturate to the destination bound.
                    if let Some(exponent_index) = prefix.find(['e', 'E']) {
                        let mantissa = &prefix[..exponent_index];
                        let nonzero = mantissa
                            .bytes()
                            .any(|byte| byte.is_ascii_digit() && byte != b'0');
                        let negative_exponent =
                            prefix.as_bytes().get(exponent_index + 1) == Some(&b'-');
                        if !nonzero || negative_exponent {
                            return Ok(if target_scale == 0 {
                                "0".to_string()
                            } else {
                                format!("0.{}", "0".repeat(target_scale as usize))
                            });
                        }
                        let target_precision = precision.unwrap_or(10);
                        let saturated = saturated_decimal_string(
                            prefix.starts_with('-'),
                            target_precision.saturating_sub(target_scale) as usize,
                            target_scale as usize,
                        );
                        return out_of_range_1264(saturated, column);
                    }
                }
            }
            return Err(original.clone());
        };
        let unsigned = formatted.strip_prefix('-').unwrap_or(&formatted);
        let integer = unsigned.split_once('.').map_or(unsigned, |(int, _)| int);
        let int_digits = integer.trim_start_matches('0').len();
        if let Some(pr) = precision {
            let max_int_digits = (pr as usize).saturating_sub(sc);
            if int_digits > max_int_digits {
                let saturated =
                    saturated_decimal_string(formatted.starts_with('-'), max_int_digits, sc);
                return out_of_range_1264(saturated, column);
            }
        }
        return Ok(formatted);
    }
    let (negative, magnitude) = parsed.expect("checked above");
    // Precision check: the integer-part digit count must fit `precision - scale`.
    // `magnitude` already carries exactly `scale` fractional digits, so the
    // integer-digit count is total digits minus the scale.
    if let Some(pr) = precision {
        let total_digits = magnitude.to_string().len();
        let int_digits = if magnitude == 0 {
            0
        } else {
            total_digits.saturating_sub(sc)
        };
        let max_int_digits = (pr as usize).saturating_sub(sc);
        if int_digits > max_int_digits {
            // MySQL saturates to the largest in-range magnitude in
            // non-strict mode (e.g. DECIMAL(4,2) → ±99.99); strict rejects.
            let saturated = saturated_decimal_string(negative, max_int_digits, sc);
            return out_of_range_1264(saturated, column);
        }
    }
    // Render the magnitude back to "[-]int.frac" with exactly `scale` digits.
    let digits = magnitude.to_string();
    let body = if sc == 0 {
        digits
    } else if digits.len() <= sc {
        format!("0.{digits:0>sc$}")
    } else {
        let point = digits.len() - sc;
        format!("{}.{}", &digits[..point], &digits[point..])
    };
    if negative && magnitude != 0 {
        Ok(format!("-{body}"))
    } else {
        Ok(body)
    }
}

// Numeric/string coercion posture. A violation is a hard error in strict
// sql_mode (mydb's default, and whenever `STRICT_*` / `TRADITIONAL` is set);
// an explicitly-configured non-strict sql_mode downgrades to MySQL's
// clamp/truncate + warning behaviour. Mirrors `temporal_violation`'s
// "empty sql_mode = strict" convention, so the default posture — and every
// existing strict test — is unchanged; only an explicit `SET sql_mode=''`
// (or a non-empty mode without `STRICT_*`) relaxes it.
thread_local! {
    static PARTITION_BOUND_COERCION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static DML_IGNORE_COERCION_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Statement-scoped coercion policy for INSERT IGNORE.
///
/// MySQL does not merely discard a row whose value is too long or out of
/// range: coercible values are truncated/clamped, a warning is emitted, and
/// the row is inserted. Keeping this as a scoped context lets every type use
/// the shared conversion kernel without adding an `ignore` parameter through
/// the storage call graph. The depth counter makes nested trigger/routine DML
/// restore the caller's policy correctly.
pub(crate) struct DmlIgnoreCoercionGuard {
    active: bool,
}

thread_local! {
    static NONTRANSACTIONAL_COERCION_DEPTH: std::cell::Cell<u16> =
        const { std::cell::Cell::new(0) };
}

struct NonTransactionalCoercionGuard {
    active: bool,
}

impl NonTransactionalCoercionGuard {
    fn new(engine: &str) -> Self {
        let active = ["MEMORY", "MYISAM", "CSV", "ARCHIVE", "BLACKHOLE"]
            .iter()
            .any(|name| engine.eq_ignore_ascii_case(name));
        if active {
            NONTRANSACTIONAL_COERCION_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        }
        Self { active }
    }
}

impl Drop for NonTransactionalCoercionGuard {
    fn drop(&mut self) {
        if self.active {
            NONTRANSACTIONAL_COERCION_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
        }
    }
}

impl DmlIgnoreCoercionGuard {
    pub(crate) fn new(active: bool) -> Self {
        if active {
            DML_IGNORE_COERCION_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        }
        Self { active }
    }
}

impl Drop for DmlIgnoreCoercionGuard {
    fn drop(&mut self) {
        if self.active {
            DML_IGNORE_COERCION_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
        }
    }
}

#[inline]
pub(crate) fn coercion_strict() -> bool {
    if PARTITION_BOUND_COERCION.with(|flag| flag.get()) {
        return true;
    }
    if DML_IGNORE_COERCION_DEPTH.with(|depth| depth.get() != 0) {
        return false;
    }
    let bits = crate::engine::expr::session_temporal_sql_mode_bits();
    if NONTRANSACTIONAL_COERCION_DEPTH.with(|depth| depth.get() != 0)
        && bits & crate::engine::api::SQL_MODE_STRICT_ALL_TABLES == 0
    {
        return false;
    }
    bits & crate::engine::api::TEMPORAL_MODE_STRICT != 0
        || bits & crate::engine::api::TEMPORAL_MODE_EXPLICIT == 0
}

/// Out-of-range numeric value: error 1264 (strict) or clamp to `clamped`
/// plus warning 1264 (non-strict), matching MySQL's saturating store.
#[cold]
fn out_of_range_1264<T>(clamped: T, column: &str) -> Result<T, QueryError> {
    if coercion_strict() {
        Err(error(
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            "out of range value for column",
        ))
    } else {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            format!("Out of range value for column '{column}' at row 1"),
        );
        Ok(clamped)
    }
}

/// BIT stores fed by an over-width binary string (including `b'…'`) and by
/// floating values outside the unsigned 64-bit conversion domain use
/// ER_DATA_TOO_LONG, not the numeric 1264 diagnostic. Unlike ordinary
/// VARCHAR truncation, INSERT IGNORE retains errno 1406 as the warning code
/// while saturating the BIT value to its all-ones representation.
#[cold]
fn bit_data_too_long_1406<T>(clamped: T, column: &str) -> Result<T, QueryError> {
    if coercion_strict() {
        Err(error(
            crate::error::catalog::ER_DATA_TOO_LONG,
            format!("data too long for column '{column}'"),
        ))
    } else {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_DATA_TOO_LONG,
            format!("Data too long for column '{column}' at row 1"),
        );
        Ok(clamped)
    }
}

/// Over-length string/blob value: error 1406 (strict) or truncate to
/// `truncated` plus warning 1265 (non-strict). `warn` is false when the
/// removed suffix is only trailing spaces (CHAR/VARCHAR), which MySQL
/// truncates silently.
#[cold]
fn too_long_1406<T>(truncated: T, column: &str, warn: bool) -> Result<T, QueryError> {
    // SQL string types ignore excess pad spaces even in strict mode. This is
    // distinct from BINARY/VARBINARY, where every byte is significant and an
    // over-width 0x20 still warns/errors.
    if !warn {
        return Ok(truncated);
    }
    if coercion_strict() {
        Err(error(
            crate::error::catalog::ER_DATA_TOO_LONG,
            format!("data too long for column '{column}'"),
        ))
    } else {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::WARN_DATA_TRUNCATED,
            format!("Data truncated for column '{column}' at row 1"),
        );
        Ok(truncated)
    }
}

/// VARCHAR differs from CHAR when an over-width suffix consists only of pad
/// spaces: the value is accepted even in strict mode, but MySQL records a
/// Note 1265 for VARCHAR. CHAR strips its pad suffix before width checking and
/// remains silent.
#[cold]
fn varchar_pad_space_truncation<T>(truncated: T, column: &str) -> T {
    crate::engine::api::push_statement_warning(
        crate::error::ConditionLevel::Note,
        crate::error::catalog::WARN_DATA_TRUNCATED,
        format!("Data truncated for column '{column}' at row 1"),
    );
    truncated
}

/// Truncate a string to at most `max_chars` characters. Returns the
/// truncated string and whether any non-space character was dropped (which
/// drives the 1265 warning; trailing-space overflow is silent in MySQL).
fn truncate_chars(text: &str, max_chars: usize) -> (String, bool) {
    let truncated: String = text.chars().take(max_chars).collect();
    let dropped_nonspace = text.chars().skip(max_chars).any(|c| c != ' ');
    (truncated, dropped_nonspace)
}

/// Truncate a UTF-8 string to at most `max_bytes` bytes on a char boundary.
fn truncate_bytes_on_char_boundary(text: &str, max_bytes: usize) -> String {
    let mut end = max_bytes.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

/// Largest in-range DECIMAL magnitude as a canonical `[-]int.frac` string:
/// `int_digits` nines, then `scale` fractional nines (e.g. (2, 2) → "99.99",
/// (0, 2) → "0.99"). Used for MySQL's non-strict saturating store.
fn saturated_decimal_string(negative: bool, int_digits: usize, scale: usize) -> String {
    let int_part = if int_digits == 0 {
        "0".to_string()
    } else {
        "9".repeat(int_digits)
    };
    let body = if scale == 0 {
        int_part
    } else {
        format!("{int_part}.{}", "9".repeat(scale))
    };
    if negative {
        format!("-{body}")
    } else {
        body
    }
}

fn check_int_range(v: i64, min: i64, max: i64, column: &str) -> Result<i64, QueryError> {
    if v < min || v > max {
        return out_of_range_1264(v.clamp(min, max), column);
    }
    Ok(v)
}

fn check_uint_range(v: u64, max: u64, column: &str) -> Result<u64, QueryError> {
    if v > max {
        return out_of_range_1264(max, column);
    }
    Ok(v)
}

/// Parse a string-like value for assignment to an integer column.
///
/// This is deliberately different from `CAST(string AS SIGNED)`: column
/// assignment treats the numeric prefix as an exact DECIMAL and rounds it to
/// the nearest integer, while CAST truncates a string at the decimal point.
/// Returning a BigInt keeps range classification exact for BIGINT UNSIGNED
/// and for inputs wider than either native integer type.
fn rounded_integer_assignment_text(
    raw: &str,
    column: &str,
) -> Result<(num_bigint::BigInt, bool), QueryError> {
    use num_bigint::BigInt;
    use num_traits::Zero;

    let (prefix, clean) = decimal_numeric_prefix(raw);
    let Some(prefix) = prefix else {
        if coercion_strict() {
            return Err(error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect integer value",
            ));
        }
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
            format!("Incorrect integer value: '{raw}' for column '{column}' at row 1"),
        );
        return Ok((BigInt::zero(), true));
    };
    if !clean && coercion_strict() {
        return Err(error(
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
            "incorrect integer value",
        ));
    }

    let rounded = crate::util::decimal::rescale_decimal_big_str(prefix, 0)
        .and_then(|value| BigInt::parse_bytes(value.as_bytes(), 10))
        .unwrap_or_else(|| {
            // An exponent beyond the bounded DECIMAL parser is necessarily
            // outside every integer destination unless its mantissa is zero.
            // Use a compact sentinel beyond u64 rather than materialising an
            // attacker-controlled power of ten.
            let nonzero = prefix
                .split_once(['e', 'E'])
                .map_or(prefix, |(mantissa, _)| mantissa)
                .bytes()
                .any(|byte| byte.is_ascii_digit() && byte != b'0');
            if !nonzero {
                return BigInt::zero();
            }
            let sentinel = BigInt::from(u64::MAX) + 1u8;
            if prefix.trim_start().starts_with('-') {
                -sentinel
            } else {
                sentinel
            }
        });
    Ok((rounded, clean))
}

fn integer_assignment_text(value: &Value) -> Option<std::borrow::Cow<'_, str>> {
    match value {
        Value::String(text) | Value::Decimal(text) => {
            Some(std::borrow::Cow::Borrowed(text.as_str()))
        }
        Value::SmallString(bytes) => {
            // SAFETY: SmallString maintains a UTF-8 invariant.
            Some(std::borrow::Cow::Borrowed(unsafe {
                std::str::from_utf8_unchecked(bytes)
            }))
        }
        Value::Bytes(bytes) => Some(String::from_utf8_lossy(bytes)),
        _ => None,
    }
}

#[cold]
fn warn_integer_assignment_truncation(column: &str) {
    crate::engine::api::push_statement_warning(
        crate::error::ConditionLevel::Warning,
        crate::error::catalog::WARN_DATA_TRUNCATED,
        format!("Data truncated for column '{column}' at row 1"),
    );
}

/// Coerce an exact DECIMAL into a signed integer without first narrowing it
/// through `i64::parse`. Values outside i64 (for example
/// 18446744073709551615.0) still need to reach the common 1264
/// strict/non-strict saturation policy rather than turning into 1366.
fn coerce_to_signed(value: Value, min: i64, max: i64, column: &str) -> Result<i64, QueryError> {
    if let Some(text) = integer_assignment_text(&value) {
        use num_bigint::BigInt;
        use num_traits::ToPrimitive;

        let (number, clean) = rounded_integer_assignment_text(&text, column)?;
        if number < BigInt::from(min) {
            return out_of_range_1264(min, column);
        }
        if number > BigInt::from(max) {
            return out_of_range_1264(max, column);
        }
        if !clean {
            warn_integer_assignment_truncation(column);
        }
        return Ok(number
            .to_i64()
            .expect("integer assignment range was checked"));
    }
    if matches!(
        &value,
        Value::DecimalCompact(..) | Value::Decimal128(..) | Value::DecimalWide(..)
    ) {
        use num_bigint::BigInt;
        use num_traits::ToPrimitive;

        let exact = value
            .exact_decimal()
            .expect("binary decimal variants are exact");
        let (rounded, _) = crate::util::decimal::rescale_exact(exact, 0, true).into_big_parts();
        if rounded < BigInt::from(min) {
            return out_of_range_1264(min, column);
        }
        if rounded > BigInt::from(max) {
            return out_of_range_1264(max, column);
        }
        return Ok(rounded
            .to_i64()
            .expect("signed integer assignment range was checked"));
    }
    let number = to_i64(value)?;
    check_int_range(number, min, max, column)
}

/// Coerce a value into an unsigned integer column of upper bound `max`.
/// A negative signed input is out of range for UNSIGNED: MySQL clamps it to
/// 0 (non-strict) or errors (strict) rather than wrapping — `to_u64` alone
/// hard-errors on negatives, so intercept them here first.
fn coerce_to_unsigned(value: Value, max: u64, column: &str) -> Result<u64, QueryError> {
    if matches!(&value, Value::Int(v) if *v < 0) {
        return out_of_range_1264(0, column);
    }
    if let Some(text) = integer_assignment_text(&value) {
        use num_bigint::BigInt;
        use num_traits::{Signed, ToPrimitive};

        let (number, clean) = rounded_integer_assignment_text(&text, column)?;
        if number.is_negative() {
            return out_of_range_1264(0, column);
        }
        if number > BigInt::from(max) {
            return out_of_range_1264(max, column);
        }
        if !clean {
            warn_integer_assignment_truncation(column);
        }
        return Ok(number
            .to_u64()
            .expect("unsigned assignment range was checked"));
    }
    if matches!(
        &value,
        Value::DecimalCompact(..) | Value::Decimal128(..) | Value::DecimalWide(..)
    ) {
        use num_bigint::BigInt;
        use num_traits::{Signed, ToPrimitive};

        let exact = value
            .exact_decimal()
            .expect("binary decimal variants are exact");
        let (rounded, _) = crate::util::decimal::rescale_exact(exact, 0, true).into_big_parts();
        if rounded.is_negative() {
            return out_of_range_1264(0, column);
        }
        if rounded > BigInt::from(max) {
            return out_of_range_1264(max, column);
        }
        return Ok(rounded
            .to_u64()
            .expect("unsigned integer assignment range was checked"));
    }
    // Check the floating-point range before Rust's saturating cast erases
    // overflow. The exclusive 2^64 boundary cannot be represented as u64.
    let float = match &value {
        Value::Float(v) => Some(f64::from(*v)),
        Value::Double(v) => Some(*v),
        _ => None,
    };
    if let Some(number) = float {
        if number < 0.0 {
            return out_of_range_1264(0, column);
        }
        if !number.is_finite() || number >= 18446744073709551616.0 || number > max as f64 {
            return out_of_range_1264(max, column);
        }
    }
    let v = to_u64(value)?;
    check_uint_range(v, max, column)
}

/// Shift `mag` from `from_scale` decimal digits to `to_scale`. Returns
/// the rescaled magnitude when it still fits the compact lane. `None` means
/// the caller must promote through the exact DECIMAL path; it is not a SQL
/// range error because the destination column may legitimately require an
/// i128 or BigInt coefficient.
fn rescale_decimal_compact(mag: i64, from_scale: u8, to_scale: u8) -> Option<i64> {
    crate::util::decimal::rescale_round_i128(i128::from(mag), from_scale, to_scale)
        .and_then(|magnitude| i64::try_from(magnitude).ok())
}

/// Enforce the declared precision (DECIMAL(P, S) allows at most `P - S`
/// integer digits). Returns `mag` unchanged when in range; in non-strict
/// mode an over-range magnitude saturates to the largest representable
/// value (warning 1264), while strict mode rejects it.
fn enforce_decimal_precision(
    mag: i64,
    scale: u8,
    precision: u8,
    column: &str,
) -> Result<i64, QueryError> {
    let max_int_digits = precision.saturating_sub(scale) as u32;
    if max_int_digits >= 18 {
        return Ok(mag);
    }
    // A compact i64 coefficient with scale >= 19 always has a zero integer
    // part.  Avoid overflowing `10_i64.pow(scale)` (debug panic / release
    // wrap) for valid definitions such as DECIMAL(30,30).
    let Some(scale_shift) = 10i64.checked_pow(scale as u32) else {
        return Ok(mag);
    };
    let int_part = (mag / scale_shift).unsigned_abs();
    let max_int = 10u64.pow(max_int_digits);
    if int_part >= max_int {
        // Largest in-range magnitude = 10^P - 1 (P = precision), signed.
        // Reaching this branch proves 10^P fits i64: otherwise an i64
        // coefficient could not have P decimal digits in the first place.
        let max_mag = 10i64
            .checked_pow(precision as u32)
            .and_then(|limit| limit.checked_sub(1))
            .expect("violating compact DECIMAL precision fits i64");
        let saturated = if mag < 0 { -max_mag } else { max_mag };
        return out_of_range_1264(saturated, column);
    }
    Ok(mag)
}

fn coerce_to_time_string(value: Value) -> Result<String, QueryError> {
    let s = match value {
        Value::String(v) | Value::Decimal(v) => v,
        Value::Bytes(v) => String::from_utf8_lossy(&v).to_string(),
        other => value_to_coercion_string(&other)?,
    };
    normalize_mysql_time_input(&s)
}

/// Normalize MySQL's accepted TIME input spellings to `[-]HH:MM:SS[.frac]`.
///
/// Besides colon-separated durations, assignment accepts compact numeric text
/// (`123456.78` → `12:34:56.78`), a day prefix (`20 10:22:33`), and the time
/// part of a datetime. Doing this before the packed TIME codec keeps string,
/// integer, DECIMAL and prepared-parameter lanes identical.
fn normalize_mysql_time_input(input: &str) -> Result<String, QueryError> {
    let trimmed = input.trim();
    let (negative, mut body) = match trimmed.strip_prefix('-') {
        Some(rest) => (true, rest.trim_start()),
        None => (false, trimmed),
    };

    let mut day_hours = 0u64;
    let fields = body.split_whitespace().collect::<Vec<_>>();
    if fields.len() == 2 {
        if fields[0].contains('-') {
            // DATETIME → TIME keeps the wall-clock fields. An explicit
            // displacement belongs to the datetime instant, not the TIME
            // duration, and is discarded with the date component.
            body = fields[1];
        } else if fields[1].contains(':') {
            let days = fields[0].parse::<u64>().map_err(|_| {
                error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
                    format!("incorrect time value: '{input}'"),
                )
            })?;
            day_hours = days.saturating_mul(24);
            body = fields[1];
        }
    } else if fields.len() > 1 {
        return Err(error(
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
            format!("incorrect time value: '{input}'"),
        ));
    }

    if body.contains(':') {
        // Strip a trailing `+HH:MM` / `-HH:MM` displacement from a datetime
        // time part. Start after HH:MM so a leading duration sign is never
        // mistaken for a displacement.
        if let Some(offset) = body
            .char_indices()
            .skip_while(|(idx, _)| *idx < 5)
            .find_map(|(idx, ch)| matches!(ch, '+' | '-').then_some(idx))
        {
            body = &body[..offset];
        }
        let parts = body.split(':').collect::<Vec<_>>();
        let canonical = match parts.as_slice() {
            [hour, minute, second] => {
                let hour = hour.parse::<u64>().unwrap_or(u64::MAX);
                format!(
                    "{}:{}:{}",
                    day_hours.saturating_add(hour).min(839),
                    minute,
                    second
                )
            }
            [hour, minute] => {
                let hour = hour.parse::<u64>().unwrap_or(u64::MAX);
                format!("{}:{}:00", day_hours.saturating_add(hour).min(839), minute)
            }
            _ => {
                return Err(error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
                    format!("incorrect time value: '{input}'"),
                ))
            }
        };
        return Ok(if negative {
            format!("-{canonical}")
        } else {
            canonical
        });
    }

    // Compact numeric TIME. The fractional part belongs to seconds, not to
    // the decimal representation of an hour (`10.22` = 00:00:10.22).
    let mut numeric = body.split('.');
    let whole = numeric.next().unwrap_or_default();
    let fraction = numeric.next();
    if numeric.next().is_some()
        || whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction
            .is_some_and(|frac| frac.is_empty() || !frac.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return Err(error(
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
            format!("incorrect time value: '{input}'"),
        ));
    }
    let (hour, minute, second) = match whole.len() {
        1..=2 => ("0", "0", whole),
        3..=4 => ("0", &whole[..whole.len() - 2], &whole[whole.len() - 2..]),
        _ => (
            &whole[..whole.len() - 4],
            &whole[whole.len() - 4..whole.len() - 2],
            &whole[whole.len() - 2..],
        ),
    };
    let hour = hour.parse::<u64>().unwrap_or(u64::MAX).min(839);
    let fraction = fraction.map_or(String::new(), |frac| format!(".{frac}"));
    let canonical = format!("{hour}:{minute}:{second}{fraction}");
    Ok(if negative {
        format!("-{canonical}")
    } else {
        canonical
    })
}

fn has_explicit_datetime_displacement(input: &str) -> bool {
    let bytes = input.trim().as_bytes();
    bytes.len() > 19
        && matches!(bytes.get(4), Some(b'-'))
        && matches!(bytes.get(7), Some(b'-'))
        && bytes[19..].iter().any(|byte| matches!(byte, b'+' | b'-'))
}

fn coerce_value_with_unsigned(
    value: Value,
    data_type: &DataTypeKind,
    unsigned: bool,
    column: &str,
) -> Result<Value, QueryError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    match data_type {
        DataTypeKind::TinyInt => {
            if unsigned {
                Ok(Value::UInt(coerce_to_unsigned(value, 255, column)?))
            } else {
                coerce_to_signed(value, -128, 127, column).map(Value::Int)
            }
        }
        DataTypeKind::SmallInt => {
            if unsigned {
                Ok(Value::UInt(coerce_to_unsigned(value, 65535, column)?))
            } else {
                coerce_to_signed(value, -32768, 32767, column).map(Value::Int)
            }
        }
        DataTypeKind::MediumInt => {
            if unsigned {
                Ok(Value::UInt(coerce_to_unsigned(value, 16_777_215, column)?))
            } else {
                coerce_to_signed(value, -8_388_608, 8_388_607, column).map(Value::Int)
            }
        }
        DataTypeKind::Int => {
            if unsigned {
                Ok(Value::UInt(coerce_to_unsigned(
                    value,
                    4_294_967_295,
                    column,
                )?))
            } else {
                coerce_to_signed(value, -2_147_483_648, 2_147_483_647, column).map(Value::Int)
            }
        }
        DataTypeKind::BigInt => {
            if unsigned {
                Ok(Value::UInt(coerce_to_unsigned(value, u64::MAX, column)?))
            } else {
                coerce_to_signed(value, i64::MIN, i64::MAX, column).map(Value::Int)
            }
        }
        DataTypeKind::Bool => {
            // BOOLEAN/BOOL is a pure alias for TINYINT(1) — signed 1-byte
            // integer, full -128..127 range, no 0/1 truthy clamp. Only the
            // TRUE/FALSE literals carry boolean semantics (evaluated to 1/0
            // upstream). Mirror the signed TINYINT path exactly.
            if unsigned {
                Ok(Value::UInt(coerce_to_unsigned(value, 255, column)?))
            } else {
                coerce_to_signed(value, -128, 127, column).map(Value::Int)
            }
        }
        DataTypeKind::Float { precision, scale } => {
            let v = coerce_floating_assignment(value, true, unsigned, column)?;
            let v = apply_float_md(v, *precision, *scale, column)?;
            Ok(Value::Float(v as f32))
        }
        DataTypeKind::Double { precision, scale } => {
            let v = coerce_floating_assignment(value, false, unsigned, column)?;
            let v = apply_float_md(v, *precision, *scale, column)?;
            Ok(Value::Double(v))
        }
        DataTypeKind::Decimal { precision, scale } => {
            // Fast path: an already-binary DecimalCompact only needs a
            // scale-rescale + precision check — no string round-trip.
            // This is the typical TPC-C path on UPDATE / INSERT whose
            // value just came out of a SELECT.
            if let Value::DecimalCompact(mag, vscale) = &value {
                let target_scale = scale.unwrap_or(0);
                if let Some(rescaled) = rescale_decimal_compact(*mag, *vscale, target_scale) {
                    if unsigned && rescaled < 0 {
                        return out_of_range_1264(Value::DecimalCompact(0, target_scale), column);
                    }
                    let rescaled = if let Some(pr) = precision {
                        enforce_decimal_precision(rescaled, target_scale, *pr, column)?
                    } else {
                        rescaled
                    };
                    return Ok(Value::DecimalCompact(rescaled, target_scale));
                }
            }
            let raw = match value {
                Value::Decimal(v) | Value::String(v) => v,
                Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(&b) }.to_string(),
                Value::Int(v) => v.to_string(),
                Value::UInt(v) => v.to_string(),
                Value::Float(v) => v.to_string(),
                Value::Double(v) => v.to_string(),
                Value::Bytes(v) => String::from_utf8_lossy(&v).to_string(),
                Value::Json(b) => crate::engine::json::unquote_text(&b),
                Value::Geometry(b) => crate::engine::geo::to_text(&b),
                Value::Vector(b) => crate::engine::vector::to_text(&b),
                Value::Null => return Ok(Value::Null),
                // Other binary variants: re-emit canonical ASCII so
                // the precision validator sees a stable shape. The
                // codec then re-binarises on the way to disk.
                Value::Date(_)
                | Value::Time(_)
                | Value::DateTime(_)
                | Value::Timestamp(_)
                | Value::Year(_) => temporal_numeric_assignment_text(&value),
                Value::DecimalCompact(..) => value.to_display_string(),
                Value::Decimal128(..) | Value::DecimalWide(..) => value.to_display_string(),
            };
            let formatted = format_decimal(&raw, *precision, *scale, column)?;
            if unsigned
                && formatted.starts_with('-')
                && formatted
                    .bytes()
                    .any(|byte| byte.is_ascii_digit() && byte != b'0')
            {
                let target_scale = scale.unwrap_or(0);
                let zero = if target_scale == 0 {
                    "0".to_string()
                } else {
                    format!("0.{}", "0".repeat(target_scale as usize))
                };
                return out_of_range_1264(Value::Decimal(zero), column);
            }
            // Try to upgrade to a binary `DecimalCompact` so the codec
            // skips the parser on its way to disk. Falls back to
            // `Value::Decimal(String)` for the rare case the magnitude
            // doesn't fit in `i64` (DECIMAL(P>18)).
            let target_scale = scale.unwrap_or(0);
            if precision.is_some_and(|precision| precision > 38) {
                let (magnitude, parsed_scale) =
                    crate::util::decimal::parse_exact_decimal_big(&formatted).ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                            "incorrect decimal value",
                        )
                    })?;
                debug_assert_eq!(parsed_scale, target_scale);
                return Ok(Value::DecimalWide(Box::new(crate::types::DecimalWide {
                    magnitude,
                    scale: target_scale,
                })));
            }
            match parse_decimal_string_to_magnitude(&formatted, target_scale) {
                Ok((negative, magnitude)) if magnitude <= i64::MAX as u128 => {
                    let signed = if negative {
                        -(magnitude as i64)
                    } else {
                        magnitude as i64
                    };
                    Ok(Value::DecimalCompact(signed, target_scale))
                }
                _ => Ok(Value::Decimal(formatted)),
            }
        }
        DataTypeKind::Geometry { subtype, srid } => {
            // Geometry columns accept only an actual geometry expression.
            // MySQL does not implicitly interpret text, raw WKB, or a BLOB
            // prepared-statement parameter as geometry.
            let bytes = match value {
                Value::Geometry(b) => crate::engine::geo::normalize_internal(&b)?,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_CANT_CREATE_GEOMETRY_OBJECT,
                        "Cannot get geometry object from data you send to the GEOMETRY field",
                    ))
                }
            };
            crate::engine::geo::validate_internal_constraints(
                &bytes,
                subtype.wkb_type_code(),
                *srid,
            )?;
            Ok(Value::Geometry(bytes))
        }
        DataTypeKind::Vector { dim, datatype } => Ok(Value::Vector(
            crate::engine::vector::codec::coerce_value(value, *datatype, *dim)?,
        )),
        DataTypeKind::Blob { max_len } => {
            let mut bytes = match value {
                Value::Bytes(v) => v,
                Value::String(v) | Value::Decimal(v) => v.into_bytes(),
                other => value_to_coercion_string(&other)?.into_bytes(),
            };
            if bytes.len() as u64 > u64::from(*max_len) {
                bytes.truncate(*max_len as usize);
                return too_long_1406(Value::Bytes(bytes), column, true);
            }
            Ok(Value::Bytes(bytes))
        }
        DataTypeKind::Binary { len } => {
            let mut bytes = match value {
                Value::Bytes(v) => v,
                Value::String(v) | Value::Decimal(v) => v.into_bytes(),
                other => value_to_coercion_string(&other)?.into_bytes(),
            };
            let max_len = len.unwrap_or(1);
            if bytes.len() > max_len {
                bytes.truncate(max_len);
                return too_long_1406(Value::Bytes(bytes), column, true);
            }
            bytes.resize(max_len, 0);
            Ok(Value::Bytes(bytes))
        }
        DataTypeKind::VarBinary { len } => {
            let mut bytes = match value {
                Value::Bytes(v) => v,
                Value::String(v) | Value::Decimal(v) => v.into_bytes(),
                other => value_to_coercion_string(&other)?.into_bytes(),
            };
            if let Some(max_len) = *len {
                if bytes.len() > max_len {
                    bytes.truncate(max_len);
                    return too_long_1406(Value::Bytes(bytes), column, true);
                }
            }
            Ok(Value::Bytes(bytes))
        }
        DataTypeKind::Char { len } => {
            let max_len = len.unwrap_or(1);
            let mut text =
                if let Some(text) = crate::types::float_text_for_char_width(&value, max_len) {
                    text
                } else {
                    match value {
                        Value::String(v) | Value::Decimal(v) => v,
                        Value::Bytes(v) => String::from_utf8_lossy(&v).to_string(),
                        other => value_to_coercion_string(&other)?,
                    }
                };
            // Removing ASCII padding preserves UTF-8 boundaries. Reuse the
            // owned input instead of copying every CHAR assignment, and use
            // byte length as a cheap upper bound on its character count.
            let trimmed_len = crate::util::pad_space::trim_trailing_spaces_len(text.as_bytes());
            text.truncate(trimmed_len);
            if text.len() > max_len && text.chars().count() > max_len {
                let (truncated, warn) = truncate_chars(&text, max_len);
                return too_long_1406(Value::String(truncated), column, warn);
            }
            Ok(Value::String(text))
        }
        DataTypeKind::VarChar { len } => {
            let text = if let Some(max_len) = *len {
                crate::types::float_text_for_char_width(&value, max_len)
            } else {
                None
            };
            let text = match text {
                Some(text) => text,
                None => match value {
                    Value::String(v) | Value::Decimal(v) => v,
                    Value::Bytes(v) => String::from_utf8_lossy(&v).to_string(),
                    other => value_to_coercion_string(&other)?,
                },
            };
            if let Some(max_len) = *len {
                if text.len() > max_len && text.chars().count() > max_len {
                    let (truncated, warn) = truncate_chars(&text, max_len);
                    if !warn {
                        return Ok(varchar_pad_space_truncation(
                            Value::String(truncated),
                            column,
                        ));
                    }
                    return too_long_1406(Value::String(truncated), column, warn);
                }
            }
            Ok(Value::String(text))
        }
        DataTypeKind::Date => {
            // Pass-through fast path for already-binary Date.
            if let Value::Date(d) = value {
                if d == ZERO_DATE_SENTINEL {
                    zero_date_policy("date")?;
                }
                return Ok(Value::Date(d));
            }
            // Validate via the canonical "YYYY-MM-DD" string then
            // upgrade to `Value::Date` so the codec encode path
            // skips its parser.
            let s = coerce_to_date_string(value)?;
            let days = match parse_date_to_days(&s) {
                Ok(days) => {
                    if days == ZERO_DATE_SENTINEL {
                        zero_date_policy("date")?;
                    }
                    days
                }
                Err(_) => match invalid_temporal_components(&s, "date", true)? {
                    Some((year, month, day)) => crate::types::encode_tagged_date(year, month, day)
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
                                format!("incorrect date value: '{s}'"),
                            )
                        })?,
                    None => ZERO_DATE_SENTINEL,
                },
            };
            Ok(Value::Date(days))
        }
        DataTypeKind::Time { fsp } => {
            let micros = match value {
                Value::Time(t) => t,
                other => {
                    let original = value_to_coercion_string(&other)?;
                    match coerce_to_time_string(other).and_then(|normalized| {
                        // Normalization has already parsed compact dates/day
                        // prefixes. Check its duration before the codec clamps
                        // it; do not reparse every TIME through chrono.
                        let hours = normalized
                            .trim_start_matches('-')
                            .split(':')
                            .next()
                            .and_then(|hours| hours.parse::<u64>().ok())
                            .unwrap_or(0);
                        if hours > 838 {
                            temporal_violation(format!("incorrect time value: '{original}'"))?;
                        }
                        parse_time_to_micros(&normalized)
                    }) {
                        Ok(micros) => quantize_time_text_micros(micros, &original, *fsp),
                        Err(err)
                            if err.is_any(&[
                                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
                                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                                crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
                            ]) =>
                        {
                            temporal_violation(format!("incorrect time value: '{original}'"))?;
                            0
                        }
                        Err(err) => return Err(err),
                    }
                }
            };
            // MySQL 5.6.4+ rounds excess fractional digits by default;
            // sql_mode TIME_TRUNCATE_FRACTIONAL switches to truncation.
            // Neither emits a warning (MySQL documented behaviour). TIME
            // is a signed duration → truncate toward zero (magnitude).
            let quantized = if temporal_mode_truncates_fractional() {
                truncate_time_micros_to_fsp(micros, *fsp)
            } else {
                round_time_micros_to_fsp(micros, *fsp)
            };
            Ok(Value::Time(quantized))
        }
        DataTypeKind::DateTime { fsp } => {
            warn_deprecated_datetime_delimiter(&value);
            let micros = match value {
                Value::DateTime(dt) => {
                    if dt == crate::types::ZERO_DATETIME {
                        zero_date_policy("datetime")?;
                    }
                    dt
                }
                other => {
                    let s = coerce_to_datetime_string(other)?;
                    let parsed = if has_explicit_datetime_displacement(&s) {
                        crate::engine::expr::parse_datetime(&Value::String(s.clone()))
                            .map(|dt| dt.and_utc().timestamp_micros())
                    } else {
                        parse_datetime_to_micros(&s)
                    };
                    match parsed {
                        Ok(micros) => {
                            if micros == crate::types::ZERO_DATETIME {
                                zero_date_policy("datetime")?;
                            }
                            quantize_datetime_text_micros(micros, &s, *fsp)
                        }
                        Err(_) => match invalid_temporal_components(&s, "datetime", true)? {
                            Some((year, month, day)) => {
                                let (hour, minute, second, micros) = parse_tagged_datetime(&s)
                                    .ok_or_else(|| {
                                        error(
                                            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
                                            format!("incorrect datetime value: '{s}'"),
                                        )
                                    })?;
                                crate::types::encode_tagged_datetime(
                                    year, month, day, hour, minute, second, micros,
                                )
                                .ok_or_else(|| {
                                    error(
                                        crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
                                        format!("incorrect datetime value: '{s}'"),
                                    )
                                })?
                            }
                            None => crate::types::ZERO_DATETIME,
                        },
                    }
                }
            };
            // MySQL's "zero datetime" sentinel is special-cased: it stays
            // at i64::MIN regardless of fsp so the read path still
            // recognises it as `0000-00-00 00:00:00`.
            let quantized = if micros == crate::types::ZERO_DATETIME
                || crate::types::decode_tagged_datetime(micros).is_some()
            {
                micros
            } else if temporal_mode_truncates_fractional() {
                // Wall-clock semantics: floor toward −∞ so pre-1970
                // values do not silently jump forward to 1970-01-01.
                truncate_datetime_micros_to_fsp(micros, *fsp)
            } else {
                round_datetime_micros_to_fsp(micros, *fsp)
            };
            // 10000-01-01 must never reach the packed representation, whose
            // year bits cannot represent it. Preserve zero/partial sentinels.
            if micros != crate::types::ZERO_DATETIME
                && crate::types::decode_tagged_datetime(micros).is_none()
                && quantized > crate::types::MAX_DATETIME_MICROS
            {
                return Err(error(
                    crate::error::catalog::ER_DATETIME_FUNCTION_OVERFLOW,
                    "Datetime function: datetime field overflow",
                ));
            }
            Ok(Value::DateTime(quantized))
        }
        DataTypeKind::Timestamp { fsp } => {
            warn_deprecated_datetime_delimiter(&value);
            // A decoded/replicated Timestamp is already an absolute UTC
            // instant. Every other temporal/text lane is a wall-clock value
            // in the active session time zone and must be normalized once.
            let (micros, already_utc) = match value {
                Value::Timestamp(ts) => {
                    if ts == crate::types::ZERO_DATETIME {
                        zero_date_policy("timestamp")?;
                    }
                    (ts, true)
                }
                Value::DateTime(dt) => {
                    if dt == crate::types::ZERO_DATETIME {
                        zero_date_policy("timestamp")?;
                    }
                    (dt, false)
                }
                other => {
                    let s = coerce_to_datetime_string(other)?;
                    let parsed = if has_explicit_datetime_displacement(&s) {
                        crate::engine::expr::parse_datetime(&Value::String(s.clone()))
                            .map(|dt| dt.and_utc().timestamp_micros())
                    } else {
                        parse_datetime_to_micros(&s)
                    };
                    match parsed {
                        Ok(micros) => {
                            if micros == crate::types::ZERO_DATETIME {
                                zero_date_policy("timestamp")?;
                            }
                            (quantize_datetime_text_micros(micros, &s, *fsp), false)
                        }
                        Err(_) => {
                            invalid_temporal_components(&s, "timestamp", false)?;
                            (crate::types::ZERO_DATETIME, false)
                        }
                    }
                }
            };
            let quantized = if micros == crate::types::ZERO_DATETIME {
                micros
            } else if temporal_mode_truncates_fractional() {
                // Wall-clock semantics: floor toward −∞ (see DateTime
                // arm above).
                truncate_datetime_micros_to_fsp(micros, *fsp)
            } else {
                round_datetime_micros_to_fsp(micros, *fsp)
            };
            let utc = if already_utc || quantized == crate::types::ZERO_DATETIME {
                quantized
            } else {
                crate::engine::expr::session_local_datetime_to_utc_timestamp_micros(quantized)?
            };
            // mydb TIMESTAMP extends through year 9999. Check the rounded
            // wall time as well as UTC so an offset cannot legalize year
            // 10000. Typed TIMESTAMP input has already been normalized.
            if utc != crate::types::ZERO_DATETIME
                && ((!already_utc && !crate::types::datetime_micros_in_calendar_range(quantized))
                    || !(crate::types::MIN_TIMESTAMP_MICROS..=crate::types::MAX_DATETIME_MICROS)
                        .contains(&utc))
            {
                return out_of_range_1264(Value::Timestamp(crate::types::ZERO_DATETIME), column);
            }
            Ok(Value::Timestamp(utc))
        }
        DataTypeKind::Year => {
            if let Value::Year(y) = value {
                return Ok(Value::Year(y));
            }
            // YEAR retains MySQL's historical distinction between numeric
            // zero and a short character zero. Numeric 0 and the explicit
            // four-digit string '0000' mean the zero year, while clean
            // one-/two-/three-digit character forms such as '0' and '00'
            // pass through the two-digit pivot and mean 2000.
            let character_zero_means_2000 = {
                let text = match &value {
                    Value::String(text) => Some(text.as_str()),
                    Value::SmallString(bytes) => {
                        Some(unsafe { std::str::from_utf8_unchecked(bytes) })
                    }
                    Value::Bytes(bytes) => std::str::from_utf8(bytes).ok(),
                    _ => None,
                };
                text.is_some_and(|text| {
                    let trimmed = text.trim();
                    let parsed = crate::util::decimal::parse_lenient_f64(trimmed);
                    trimmed != "0000" && !trimmed.is_empty() && parsed.clean && parsed.value == 0.0
                })
            };
            let v = match &value {
                Value::Date(_) | Value::DateTime(_) | Value::Timestamp(_) => {
                    let rendered = value_to_coercion_string(&value)?;
                    rendered
                        .get(..4)
                        .and_then(|year| year.parse::<i64>().ok())
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                                "incorrect YEAR value",
                            )
                        })?
                }
                // A typed TIME is applied to the statement's current date
                // before extracting YEAR. With SET timestamp at
                // 2011-12-31, TIME '25:00:00' therefore stores 2012.
                Value::Time(micros) => crate::engine::expr::session_now()
                    .date()
                    .and_hms_opt(0, 0, 0)
                    .and_then(|midnight| {
                        midnight.checked_add_signed(chrono::Duration::microseconds(*micros))
                    })
                    .and_then(|datetime| datetime.format("%Y").to_string().parse::<i64>().ok())
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                            "out of range value for column",
                        )
                    })?,
                Value::String(text) | Value::Decimal(text)
                    if text.len() >= 10
                        && text.as_bytes().get(4) == Some(&b'-')
                        && text.as_bytes()[..4].iter().all(u8::is_ascii_digit) =>
                {
                    text[..4].parse::<i64>().map_err(|_| {
                        error(
                            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                            "incorrect YEAR value",
                        )
                    })?
                }
                Value::String(text) | Value::Decimal(text) if text.contains(':') => {
                    let micros = parse_time_to_micros(text)?;
                    crate::engine::expr::session_now()
                        .date()
                        .and_hms_opt(0, 0, 0)
                        .and_then(|midnight| {
                            midnight.checked_add_signed(chrono::Duration::microseconds(micros))
                        })
                        .and_then(|datetime| datetime.format("%Y").to_string().parse::<i64>().ok())
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                                "out of range value for column",
                            )
                        })?
                }
                // YEAR uses integer *assignment* coercion, not CAST(... AS
                // SIGNED) coercion: decimal and floating inputs are rounded
                // to the nearest integer, scientific notation is accepted,
                // and a usable numeric prefix stores its rounded value with
                // warning 1265 when trailing garbage was discarded.
                Value::String(text) | Value::Decimal(text) => {
                    use num_traits::ToPrimitive;

                    let (number, clean) = rounded_integer_assignment_text(text, column)?;
                    if !clean {
                        warn_integer_assignment_truncation(column);
                    }
                    number.to_i64().unwrap_or_else(|| {
                        if number.sign() == num_bigint::Sign::Minus {
                            i64::MIN
                        } else {
                            i64::MAX
                        }
                    })
                }
                Value::SmallString(bytes) => {
                    use num_traits::ToPrimitive;

                    let text = unsafe { std::str::from_utf8_unchecked(bytes) };
                    let (number, clean) = rounded_integer_assignment_text(text, column)?;
                    if !clean {
                        warn_integer_assignment_truncation(column);
                    }
                    number.to_i64().unwrap_or_else(|| {
                        if number.sign() == num_bigint::Sign::Minus {
                            i64::MIN
                        } else {
                            i64::MAX
                        }
                    })
                }
                Value::Bytes(bytes) => {
                    use num_traits::ToPrimitive;

                    let text = String::from_utf8_lossy(bytes);
                    let (number, clean) = rounded_integer_assignment_text(&text, column)?;
                    if !clean {
                        warn_integer_assignment_truncation(column);
                    }
                    number.to_i64().unwrap_or_else(|| {
                        if number.sign() == num_bigint::Sign::Minus {
                            i64::MIN
                        } else {
                            i64::MAX
                        }
                    })
                }
                Value::DecimalCompact(magnitude, scale) => {
                    let rounded =
                        crate::util::decimal::rescale_round_i128(*magnitude as i128, *scale, 0)
                            .unwrap_or(if *magnitude < 0 { i128::MIN } else { i128::MAX });
                    i64::try_from(rounded).unwrap_or(if rounded < 0 { i64::MIN } else { i64::MAX })
                }
                Value::Float(number) => {
                    if number.is_finite() {
                        number.round() as i64
                    } else if number.is_sign_negative() {
                        i64::MIN
                    } else {
                        i64::MAX
                    }
                }
                Value::Double(number) => {
                    if number.is_finite() {
                        number.round() as i64
                    } else if number.is_sign_negative() {
                        i64::MIN
                    } else {
                        i64::MAX
                    }
                }
                _ => to_i64(value)?,
            };
            if v == 0 {
                return Ok(Value::Year(if character_zero_means_2000 { 100 } else { 0 }));
            }
            if (1901..=2155).contains(&v) {
                Ok(Value::Year((v - 1900) as u8))
            } else if (1..=69).contains(&v) {
                Ok(Value::Year((v + 100) as u8))
            } else if (70..=99).contains(&v) {
                Ok(Value::Year(v as u8))
            } else {
                // MySQL stores 0000 for an out-of-range YEAR in non-strict
                // mode (warning 1264); strict rejects.
                out_of_range_1264(Value::Year(0), column)
            }
        }
        DataTypeKind::Enum { values } => {
            if matches!(&value, Value::Bytes(bytes) if bytes.is_empty()) {
                return Ok(enum_zero_value());
            }
            // Numeric input path — MySQL 8.0 semantics:
            //   * 0 → '' (the empty-index sentinel) plus 1265 truncation
            //   * 1..=N → the N-th declared member
            //   * anything else → 1265 truncation.
            // Both Int and UInt must land here; the prepared-statement
            // protocol frequently delivers unsigned parameters.
            let numeric_ordinal: Option<i128> = match &value {
                Value::UInt(v) => Some(*v as i128),
                Value::Int(v) => Some(*v as i128),
                _ => None,
            };
            if let Some(ord) = numeric_ordinal {
                if ord == 0 {
                    return enum_set_truncation(enum_zero_value(), "ENUM", column);
                }
                if ord >= 1 && (ord as usize) <= values.len() {
                    return Ok(enum_member_value(values[(ord - 1) as usize].clone()));
                }
                return enum_set_truncation(enum_zero_value(), "ENUM", column);
            }
            let s = match value {
                Value::String(v) | Value::Decimal(v) => v,
                Value::Bytes(v) => String::from_utf8_lossy(&v).to_string(),
                other => value_to_coercion_string(&other)?,
            };
            // Collation is not threaded here yet (see coerce_value_with_unsigned
            // signature); enum_lookup_member falls back to ASCII case-insensitive
            // matching when collation is None. The collation-aware path is
            // exercised by the higher-level coerce_column_value_for_schema
            // callers that carry ColumnSchema.
            if let Some(matched) = enum_assignment_member(&s, values, None) {
                Ok(enum_member_value(matched))
            } else {
                enum_set_truncation(enum_zero_value(), "ENUM", column)
            }
        }
        DataTypeKind::Set { values: allowed } => {
            // Numeric input path — MySQL 8.0 semantics: the value is a
            // little-endian bitmask over the declared members, so
            // `INSERT INTO s VALUES (5)` on `SET('a','b','c')` stores
            // `'a,c'`. Bits beyond `values.len()` are silently ignored
            // (matches MySQL's non-strict mode).
            let numeric_mask: Option<u64> = match &value {
                Value::UInt(v) => Some(*v),
                Value::Int(v) if *v >= 0 => Some(*v as u64),
                Value::Int(_) => {
                    return enum_set_truncation(Value::String(String::new()), "SET", column);
                }
                _ => None,
            };
            if let Some(mask) = numeric_mask {
                return Ok(Value::String(set_bitmask_to_csv(mask, allowed)));
            }
            let s = match value {
                Value::String(v) | Value::Decimal(v) => v,
                Value::Bytes(v) => String::from_utf8_lossy(&v).to_string(),
                other => value_to_coercion_string(&other)?,
            };
            // A quoted numeric string that is not itself a declared member
            // falls back to SET's numeric bitmask semantics. This is why
            // `"100"` on SET('1', ... '64') stores members 3,6,7.
            if !s.contains(',') {
                let trimmed = s.trim_end_matches(' ');
                if let Some(matched) = enum_lookup_member(trimmed, allowed, None) {
                    return Ok(Value::String(matched));
                }
                if let Ok(mask) = trimmed.parse::<u64>() {
                    return Ok(Value::String(set_bitmask_to_csv(mask, allowed)));
                }
            }
            // Split on ',' then trim trailing spaces only (MySQL 8.0
            // rstrips per-token; a leading space in `'a ,b'` should
            // fail 1265, not silently match `'a'`).
            let mut result: Vec<String> = Vec::new();
            let mut truncated = false;
            for part in s.split(',') {
                let trimmed = part.trim_end_matches(' ');
                if trimmed.is_empty() {
                    continue;
                }
                if let Some(matched) = enum_lookup_member(trimmed, allowed, None) {
                    if !result.iter().any(|v: &String| v == &matched) {
                        result.push(matched);
                    }
                } else {
                    truncated = true;
                }
            }
            // MySQL always stores/echoes SET members in declaration
            // order regardless of input order.
            result.sort_by_key(|m| allowed.iter().position(|v| v == m).unwrap_or(usize::MAX));
            let value = Value::String(result.join(","));
            if truncated {
                enum_set_truncation(value, "SET", column)
            } else {
                Ok(value)
            }
        }
        DataTypeKind::Bit { len } => {
            if matches!(value, Value::Null) {
                return Ok(Value::Null);
            }
            // BIT(M) accepts an integer, a bit-literal `b'…'`, or a hex
            // literal `0x…` / `x'…'`. The literals arrive as `Value::Bytes`
            // and are read as a big-endian unsigned integer (MySQL's bit
            // value); an integer keeps its magnitude. The value is stored as
            // `ceil(M/8)` big-endian bytes and read back as an integer, so
            // BIT is numeric in expression context (`b + 0`).
            let max_bits = len.unwrap_or(1);
            let max_value = if max_bits == 64 {
                u64::MAX
            } else {
                (1u64 << max_bits) - 1
            };
            let n: u64 = match value {
                Value::UInt(v) => v,
                Value::Int(v) if v >= 0 => v as u64,
                Value::Int(_) => {
                    return out_of_range_1264(Value::UInt(0), column);
                }
                Value::Bytes(ref b) => {
                    if b.len() > 8 {
                        return bit_data_too_long_1406(Value::UInt(max_value), column);
                    }
                    let n = b.iter().fold(0u64, |acc, &byte| (acc << 8) | byte as u64);
                    if n > max_value {
                        return bit_data_too_long_1406(Value::UInt(max_value), column);
                    }
                    n
                }
                Value::Float(v) => {
                    let v = v as f64;
                    if !v.is_finite() || v > u64::MAX as f64 || v < i64::MIN as f64 {
                        return bit_data_too_long_1406(Value::UInt(max_value), column);
                    }
                    if v < 0.0 {
                        return out_of_range_1264(Value::UInt(0), column);
                    }
                    v.round() as u64
                }
                Value::Double(v) => {
                    if !v.is_finite() || v > u64::MAX as f64 || v < i64::MIN as f64 {
                        return bit_data_too_long_1406(Value::UInt(max_value), column);
                    }
                    if v < 0.0 {
                        return out_of_range_1264(Value::UInt(0), column);
                    }
                    v.round() as u64
                }
                // A numeric string / decimal coerces through its integer value.
                other => {
                    let v = to_i64(other)?;
                    if v < 0 {
                        return out_of_range_1264(Value::UInt(0), column);
                    }
                    v as u64
                }
            };
            if n > max_value {
                return if max_bits == 1 {
                    out_of_range_1264(Value::UInt(max_value), column)
                } else {
                    bit_data_too_long_1406(Value::UInt(max_value), column)
                };
            }
            Ok(Value::UInt(n))
        }
        DataTypeKind::Text { max_len } => {
            let s = match value {
                Value::String(v) | Value::Decimal(v) => v,
                Value::Bytes(v) => String::from_utf8_lossy(&v).to_string(),
                other => value_to_coercion_string(&other)?,
            };
            // MySQL TEXT-family caps are byte-based (TINYTEXT = 255 bytes).
            if s.len() as u64 > u64::from(*max_len) {
                let truncated = truncate_bytes_on_char_boundary(&s, *max_len as usize);
                let warn = s[truncated.len()..].chars().any(|c| c != ' ');
                return too_long_1406(Value::String(truncated), column, warn);
            }
            Ok(Value::String(s))
        }
        DataTypeKind::Json => {
            if matches!(value, Value::Null | Value::Json(_)) {
                return Ok(value);
            }
            crate::engine::json::column_value(&value, column)
                .map(|bytes| Value::Json(bytes.into_owned()))
        }
    }
}

/// MySQL's numeric context for temporal values removes date/time separators
/// rather than parsing their display text as an ordinary decimal:
/// `10:11:12` -> `101112`, datetime -> `YYYYMMDDHHMMSS.ffffff`.
fn temporal_numeric_assignment_text(value: &Value) -> String {
    let display = value.to_display_string();
    let negative = display.starts_with('-');
    let mut output = String::with_capacity(display.len());
    if negative {
        output.push('-');
    }
    let mut saw_dot = false;
    for byte in display.bytes() {
        if byte.is_ascii_digit() {
            output.push(byte as char);
        } else if byte == b'.' && !saw_dot {
            output.push('.');
            saw_dot = true;
        }
    }
    if output.is_empty() || output == "-" {
        output.push('0');
    }
    output
}

/// First member of offset + N * step strictly above the stored high-water
/// mark. MySQL ignores an offset larger than the increment for allocation.
/// Keep the default step=1 path to one checked addition.
fn next_auto_increment_after(current: u64, step: u64, offset: u64) -> Result<u64, QueryError> {
    let next = if step == 1 {
        current.checked_add(1)
    } else {
        let offset = if offset > step { 1 } else { offset };
        if current < offset {
            Some(offset)
        } else {
            current.checked_add(step - (current - offset) % step)
        }
    };
    next.ok_or_else(|| {
        error(
            crate::error::catalog::ER_AUTOINC_READ_FAILED,
            "auto-increment value is out of range",
        )
    })
}

fn should_assign_auto_increment(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Int(0) | Value::UInt(0) => {
            // `NO_AUTO_VALUE_ON_ZERO`: an explicit 0 is stored verbatim
            // rather than triggering a fresh sequence value (only NULL
            // triggers it). MySQL 8.0 semantics.
            crate::engine::expr::session_temporal_sql_mode_bits()
                & crate::engine::api::SQL_MODE_NO_AUTO_VALUE_ON_ZERO
                == 0
        }
        _ => false,
    }
}

/// Finish assignment-only TIMESTAMP rules for a value that LOAD DATA already
/// parsed and converted from the session wall clock to UTC. This reuses the
/// exact fsp, zero-date and extended TIMESTAMP domain oracle without
/// rerunning full-row coercion over every other canonical column.
pub(crate) fn canonicalize_loaded_timestamp_value(
    micros: i64,
    wall_micros: i64,
    fsp: u8,
    column: &str,
) -> Result<Value, QueryError> {
    // LOAD DATA normalizes before assignment. Retain the same rounded wall
    // calendar check as ordinary coercion without resolving the zone again.
    // Only the final second can overflow, keeping the bulk path inexpensive.
    let micros = if wall_micros > crate::types::MAX_DATETIME_MICROS - 1_000_000 {
        let wall = if temporal_mode_truncates_fractional() {
            truncate_datetime_micros_to_fsp(wall_micros, fsp)
        } else {
            round_datetime_micros_to_fsp(wall_micros, fsp)
        };
        if wall > crate::types::MAX_DATETIME_MICROS {
            crate::types::MAX_DATETIME_MICROS + 1
        } else {
            micros
        }
    } else {
        micros
    };
    coerce_value_with_unsigned(
        Value::Timestamp(micros),
        &DataTypeKind::Timestamp { fsp },
        false,
        column,
    )
}

fn explicit_auto_increment_value_with_unsigned(
    value: Value,
    column: &ColumnSchema,
) -> Result<Option<u64>, QueryError> {
    match coerce_value_with_unsigned(value, &column.data_type, column.unsigned, &column.name)? {
        Value::Int(value) if value > 0 => Ok(Some(value as u64)),
        Value::UInt(value) if value > 0 => Ok(Some(value)),
        _ => Ok(None),
    }
}

fn auto_increment_value_from_key_part(part: &KeyPart) -> Result<Option<u64>, QueryError> {
    match part {
        KeyPart::Int(value) => Ok(Some((*value).max(0) as u64)),
        KeyPart::UInt(value) => Ok(Some(*value)),
        _ => Ok(None),
    }
}

/// Assignment-context floating conversion.
///
/// Expression arithmetic intentionally uses MySQL's lenient numeric-prefix
/// coercion, while a column assignment additionally depends on sql_mode and
/// the destination's signedness/range. Keeping that policy here avoids
/// weakening the shared storage `to_f64` primitive or duplicating it in each
/// DML executor.
fn coerce_floating_assignment(
    value: Value,
    single_precision: bool,
    unsigned: bool,
    column: &str,
) -> Result<f64, QueryError> {
    let parsed = match value {
        Value::String(text) | Value::Decimal(text) => {
            coerce_floating_text_assignment(&text, column)?
        }
        Value::SmallString(bytes) => coerce_floating_text_assignment(
            unsafe { std::str::from_utf8_unchecked(&bytes) },
            column,
        )?,
        Value::Bytes(bytes) => {
            coerce_floating_text_assignment(&String::from_utf8_lossy(&bytes), column)?
        }
        other => to_f64(other)?,
    };

    // DOUBLE literals beyond the IEEE finite range are illegal values, not a
    // storable infinity. FLOAT overflow is the ordinary column range error.
    if !parsed.is_finite() {
        if single_precision {
            let saturated = if parsed.is_sign_negative() {
                -(f32::MAX as f64)
            } else {
                f32::MAX as f64
            };
            return out_of_range_1264(saturated, column);
        }
        return Err(error(
            crate::error::catalog::ER_ILLEGAL_VALUE_FOR_TYPE,
            format!("Illegal double value for column '{column}'"),
        ));
    }
    if unsigned && parsed < 0.0 {
        return out_of_range_1264(0.0, column);
    }
    if single_precision && parsed.abs() > f32::MAX as f64 {
        let saturated = if parsed.is_sign_negative() {
            -(f32::MAX as f64)
        } else {
            f32::MAX as f64
        };
        return out_of_range_1264(saturated, column);
    }
    Ok(parsed)
}

fn coerce_floating_text_assignment(text: &str, column: &str) -> Result<f64, QueryError> {
    if coercion_strict() {
        return text.parse::<f64>().map_err(|_| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect numeric value",
            )
        });
    }
    let parsed = crate::util::decimal::parse_lenient_f64(text);
    if !parsed.clean {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
            format!("Incorrect double value for column '{column}' at row 1"),
        );
    }
    Ok(parsed.value)
}

/// Apply a `FLOAT(M,D)` / `DOUBLE(M,D)` declared scale and precision to a
/// stored value, mirroring MySQL: the value is rounded (half away from
/// zero) to `D` fractional digits, then range-checked so its integer part
/// fits in `M - D` digits. A single-argument `FLOAT(p)` (bit-precision
/// hint, no `scale`) leaves the IEEE-754 value untouched — MySQL only
/// rounds when both `M` and `D` are declared. Out-of-range values raise
/// error 1264, consistent with the integer coercion paths above.
fn apply_float_md(
    v: f64,
    precision: Option<u8>,
    scale: Option<u8>,
    column: &str,
) -> Result<f64, QueryError> {
    let Some(d) = scale else {
        return Ok(v);
    };
    if !v.is_finite() {
        return Ok(v);
    }
    // `powi` accumulates enough error at large exponents to turn the exact
    // decimal boundary 1e200 into 1.0000000000000005e200. `powf` follows the
    // correctly-rounded libm path here and avoids formatting/parsing or heap
    // allocation on the assignment path.
    let decimal_power = |exponent: i32| 10f64.powf(f64::from(exponent));
    let factor = decimal_power(d as i32);
    let rounded = (v * factor).round() / factor;
    if let Some(m) = precision {
        // Integer-part digit budget is `M - D`; the largest legal
        // magnitude is `10^(M-D) - 10^(-D)`, so `|rounded|` must stay
        // strictly below `10^(M-D)`.
        let int_digits = (m as i32) - (d as i32);
        if int_digits < 0 {
            return out_of_range_1264(0.0, column);
        }
        let limit = decimal_power(int_digits);
        // With fractional digits, the exact boundary has no representable
        // slot (`FLOAT(4,2)` tops out at 99.99), so equality is out of range.
        // At D=0 MySQL accepts the IEEE value at 10^M and only saturates
        // larger magnitudes (legacy FLOAT(30,0)/DOUBLE(200,0) behaviour).
        if rounded.abs() > limit || (d > 0 && rounded.abs() == limit) {
            // Non-strict: saturate to ±(10^(M-D) - 10^(-D)); strict rejects.
            let unit = decimal_power(-(d as i32));
            let max_mag = if limit - unit < limit {
                limit - unit
            } else {
                limit
            };
            let saturated = if rounded < 0.0 { -max_mag } else { max_mag };
            return out_of_range_1264(saturated, column);
        }
    }
    Ok(rounded)
}

/// Maximum representable AUTO_INCREMENT value for the column's
/// declared integer width and signedness. Used by the reservation
/// fast path to perform a single u64 bounds check per row instead
/// of going through `coerce_value_with_unsigned`'s full enum match.
/// `validate_auto_increment_schema` already rejects non-integer
/// AUTO_INCREMENT columns at CREATE TABLE / ALTER time, so an
/// unexpected variant here is an invariant violation rather than
/// user error.
fn auto_increment_type_max(data_type: &DataTypeKind, unsigned: bool) -> Result<u64, QueryError> {
    Ok(match (data_type, unsigned) {
        (DataTypeKind::TinyInt, false) => 127,
        (DataTypeKind::TinyInt, true) => 255,
        (DataTypeKind::SmallInt, false) => 32_767,
        (DataTypeKind::SmallInt, true) => 65_535,
        (DataTypeKind::MediumInt, false) => 8_388_607,
        (DataTypeKind::MediumInt, true) => 16_777_215,
        (DataTypeKind::Int, false) => 2_147_483_647,
        (DataTypeKind::Int, true) => 4_294_967_295,
        (DataTypeKind::BigInt, false) => i64::MAX as u64,
        (DataTypeKind::BigInt, true) => u64::MAX,
        _ => {
            return Err(error(
                crate::error::catalog::ER_WRONG_FIELD_SPEC,
                "AUTO_INCREMENT column must use an integer type",
            ));
        }
    })
}

fn generated_auto_increment_value(value: u64, column: &ColumnSchema) -> Result<Value, QueryError> {
    let raw = if column.unsigned {
        Value::UInt(value)
    } else {
        let value = i64::try_from(value).map_err(|_| {
            error(
                crate::error::catalog::ER_AUTOINC_READ_FAILED,
                "auto-increment value is out of range",
            )
        })?;
        Value::Int(value)
    };
    coerce_value_with_unsigned(raw, &column.data_type, column.unsigned, &column.name)
}

fn validate_auto_increment_schema(table: &TableSchema) -> Result<(), QueryError> {
    let auto_columns = table
        .columns
        .iter()
        .filter(|column| column.state != ColumnState::Dropped && column.auto_increment)
        .collect::<Vec<_>>();
    if auto_columns.is_empty() {
        return Ok(());
    }
    if auto_columns.len() > 1 {
        return Err(error(crate::error::catalog::ER_WRONG_AUTO_KEY,
            "Incorrect table definition; there can be only one auto column and it must be defined as a key",
        ));
    }
    let column = auto_columns[0];
    if !matches!(
        column.data_type,
        DataTypeKind::TinyInt
            | DataTypeKind::SmallInt
            | DataTypeKind::MediumInt
            | DataTypeKind::Int
            | DataTypeKind::BigInt
    ) {
        return Err(error(
            crate::error::catalog::ER_WRONG_FIELD_SPEC,
            "AUTO_INCREMENT column must use an integer type",
        ));
    }
    if column.nullable {
        return Err(error(crate::error::catalog::ER_WRONG_AUTO_KEY,
            "Incorrect table definition; AUTO_INCREMENT column must be NOT NULL and defined as a key",
        ));
    }
    if column.default.is_some() || column.default_expr.is_some() || column.on_update_expr.is_some()
    {
        return Err(error(
            crate::error::catalog::ER_INVALID_DEFAULT,
            format!("Invalid default value for '{}'", column.name),
        ));
    }
    let column_name = normalize_name(&column.name);
    let keyed = table.indexes.iter().any(|index| {
        index.state == IndexState::Public
            && index
                .columns
                .first()
                .is_some_and(|name| normalize_name(name) == column_name)
    });
    if !keyed {
        return Err(error(crate::error::catalog::ER_WRONG_AUTO_KEY,
            "Incorrect table definition; there can be only one auto column and it must be defined as a key",
        ));
    }
    for (_, dependent) in table.iter_visible_columns() {
        if let Some(sql) = &dependent.default_expr {
            let expr = crate::parser::parse_catalog_expr(sql)
                .map_err(|err| error(crate::error::catalog::ER_PARSE_ERROR, err.to_string()))?;
            if expr_references_column(&expr, &column.name) {
                return Err(error(crate::error::catalog::ER_DEFAULT_VAL_GENERATED_REF_AUTO_INC,
                    format!("Default value expression of column '{}' cannot refer to an auto-increment column", dependent.name)));
            }
        }
    }
    Ok(())
}

fn dedup_row_ids(ids: &mut Vec<RowId>) {
    // Phase B #B4: preserve insertion order while removing duplicates
    // so range/lookup index scans return rowids in *index order*.
    // The previous sort_unstable+dedup was simpler but reordered to
    // RowId numeric order, which broke the streaming aggregate's
    // assumption that GROUP BY input is grouped by index key.
    use std::collections::HashSet;
    let mut seen: HashSet<RowId> = HashSet::with_capacity(ids.len());
    ids.retain(|id| seen.insert(*id));
}

fn validate_columns(columns: &[String], table_columns: &[ColumnSchema]) -> Result<(), QueryError> {
    for column in columns {
        if !table_columns
            .iter()
            .any(|existing| normalize_name(&existing.name) == normalize_name(column))
        {
            return Err(error(
                crate::error::catalog::ER_KEY_COLUMN_DOES_NOT_EXITS,
                format!("key column '{column}' does not exist"),
            ));
        }
    }
    Ok(())
}

fn normalized_index_prefix_lengths(
    mut prefix_lengths: Vec<Option<usize>>,
    len: usize,
) -> Vec<Option<usize>> {
    prefix_lengths.resize(len, None);
    prefix_lengths.truncate(len);
    prefix_lengths
}

/// MySQL 8.0 InnoDB DDL-time index limits (ER 1070 / 1071 / 1069).
/// Enforced only when an index is created or revalidated by DDL —
/// catalogs loaded from disk are never re-checked, so pre-existing
/// indexes built before these limits landed keep working.
const MAX_INDEX_KEY_PARTS: usize = 16;
const MAX_INDEX_KEY_BYTES: u64 = 3072;
const MAX_INDEXES_PER_TABLE: usize = 64;

/// Escape hatch preserving mydb's historical superset capability of
/// full-column B-tree keys over BLOB/TEXT (MySQL requires a prefix
/// length, ER 1170). Default OFF = MySQL-aligned.
fn lob_full_column_index_allowed() -> bool {
    static ALLOWED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ALLOWED.get_or_init(|| {
        std::env::var("MYDB_LOB_FULL_COLUMN_INDEX")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
            .unwrap_or(false)
    })
}

/// ER 1069: total non-PRIMARY indexes a table may carry after a DDL.
fn validate_table_index_count(non_primary_count: usize) -> Result<(), QueryError> {
    if non_primary_count > MAX_INDEXES_PER_TABLE {
        return Err(error(
            crate::error::catalog::ER_TOO_MANY_KEYS,
            format!("Too many keys specified; max {MAX_INDEXES_PER_TABLE} keys allowed"),
        ));
    }
    Ok(())
}

/// MySQL packed-DECIMAL width: 4 bytes per 9 digits plus a remainder
/// table, applied independently to the integer and fraction parts.
fn decimal_packed_bytes(precision: u8, scale: u8) -> u64 {
    const DIG2BYTES: [u64; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
    let intg = precision.saturating_sub(scale) as u64;
    let frac = scale as u64;
    (intg / 9) * 4
        + DIG2BYTES[(intg % 9) as usize]
        + (frac / 9) * 4
        + DIG2BYTES[(frac % 9) as usize]
}

/// Maximum on-disk key bytes a single B-tree key part can contribute,
/// using MySQL's storage widths (not mydb's internal memcmp encoding)
/// so the ER 1071 threshold trips on exactly the same definitions.
/// Character prefixes count `chars × mbmaxlen`; binary prefixes count
/// bytes. Returns 0 for types whose indexability is governed by other
/// checks (JSON / GEOMETRY / VECTOR) and for superset full-column LOB
/// keys, which have no MySQL-side width to account for.
fn index_key_part_max_bytes(column: &ColumnSchema, prefix_len: Option<usize>) -> u64 {
    let mbmaxlen = || -> u64 {
        let name = column.charset.as_deref().unwrap_or("utf8mb4");
        crate::charset::resolve_charset(name)
            .map(|c| c.max_len as u64)
            .unwrap_or(4)
    };
    let char_part = |declared: usize| -> u64 {
        let chars = prefix_len.map_or(declared, |p| p.min(declared));
        chars as u64 * mbmaxlen()
    };
    let byte_part =
        |declared: usize| -> u64 { prefix_len.map_or(declared, |p| p.min(declared)) as u64 };
    let fsp_bytes = |fsp: u8| (fsp as u64 + 1) / 2;
    match &column.data_type {
        DataTypeKind::TinyInt | DataTypeKind::Bool | DataTypeKind::Year => 1,
        DataTypeKind::SmallInt => 2,
        DataTypeKind::MediumInt => 3,
        DataTypeKind::Int => 4,
        DataTypeKind::BigInt => 8,
        DataTypeKind::Float { .. } => 4,
        DataTypeKind::Double { .. } => 8,
        DataTypeKind::Decimal { precision, scale } => {
            decimal_packed_bytes(precision.unwrap_or(10), scale.unwrap_or(0))
        }
        DataTypeKind::Char { len } => char_part(len.unwrap_or(1)),
        DataTypeKind::VarChar { len } => char_part(len.unwrap_or(255)),
        DataTypeKind::Binary { len } => byte_part(len.unwrap_or(1)),
        DataTypeKind::VarBinary { len } => byte_part(len.unwrap_or(255)),
        // LOB key parts reach here only with a prefix (ER 1170 fires
        // first otherwise) or under the full-column escape hatch.
        DataTypeKind::Text { .. } => prefix_len.map_or(0, |p| p as u64 * mbmaxlen()),
        DataTypeKind::Blob { .. } => prefix_len.map_or(0, |p| p as u64),
        DataTypeKind::Date => 3,
        DataTypeKind::Time { fsp } => 3 + fsp_bytes(*fsp),
        DataTypeKind::DateTime { fsp } => 5 + fsp_bytes(*fsp),
        DataTypeKind::Timestamp { fsp } => 4 + fsp_bytes(*fsp),
        DataTypeKind::Enum { values } => crate::storage::enum_storage_width(values.len()) as u64,
        DataTypeKind::Set { values } => crate::storage::set_storage_width(values.len()) as u64,
        DataTypeKind::Bit { len } => (len.unwrap_or(1) as u64).div_ceil(8),
        DataTypeKind::Json | DataTypeKind::Geometry { .. } | DataTypeKind::Vector { .. } => 0,
    }
}

fn validate_index_key_parts(
    columns: &[String],
    prefix_lengths: &[Option<usize>],
    table_columns: &[ColumnSchema],
    index_type: &str,
) -> Result<(), QueryError> {
    if columns.len() > MAX_INDEX_KEY_PARTS {
        return Err(error(
            crate::error::catalog::ER_TOO_MANY_KEY_PARTS,
            format!("Too many key parts specified; max {MAX_INDEX_KEY_PARTS} parts allowed"),
        ));
    }
    for (pos, prefix_len) in prefix_lengths.iter().enumerate() {
        let Some(prefix_len) = prefix_len else {
            continue;
        };
        if *prefix_len == 0 {
            return Err(error(
                crate::error::catalog::ER_WRONG_SUB_KEY,
                "index prefix length must be greater than zero",
            ));
        }
        let Some(column_name) = columns.get(pos) else {
            return Err(error(
                crate::error::catalog::ER_KEY_COLUMN_DOES_NOT_EXITS,
                "index prefix column is out of range",
            ));
        };
        let Some(column) = table_columns
            .iter()
            .find(|existing| normalize_name(&existing.name) == normalize_name(column_name))
        else {
            return Err(error(
                crate::error::catalog::ER_KEY_COLUMN_DOES_NOT_EXITS,
                format!("key column '{column_name}' does not exist"),
            ));
        };
        if !matches!(
            column.data_type,
            DataTypeKind::Char { .. }
                | DataTypeKind::VarChar { .. }
                | DataTypeKind::Text { .. }
                | DataTypeKind::Binary { .. }
                | DataTypeKind::VarBinary { .. }
                | DataTypeKind::Blob { .. }
        ) {
            return Err(error(
                crate::error::catalog::ER_WRONG_SUB_KEY,
                format!("prefix length is not allowed for key column '{column_name}'"),
            ));
        }
    }
    if index_type.eq_ignore_ascii_case("SPATIAL") {
        validate_spatial_columns_not_null(columns, table_columns)?;
    }
    if index_type.eq_ignore_ascii_case("FULLTEXT") {
        for name in columns {
            let column = table_columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_BAD_FT_COLUMN,
                        format!("Column '{name}' cannot be part of FULLTEXT index"),
                    )
                })?;
            if !matches!(
                column.data_type,
                DataTypeKind::Char { .. }
                    | DataTypeKind::VarChar { .. }
                    | DataTypeKind::Text { .. }
            ) {
                return Err(error(
                    crate::error::catalog::ER_BAD_FT_COLUMN,
                    format!("Column '{name}' cannot be part of FULLTEXT index"),
                ));
            }
        }
    }
    // ER 1170 / 1071 are B-tree key-width rules; FULLTEXT keys span
    // whole LOB columns by design and SPATIAL/HNSW key shapes are
    // validated by their dedicated checks.
    if !index_type.eq_ignore_ascii_case("BTREE") {
        return Ok(());
    }
    let mut total_bytes: u64 = 0;
    for (pos, column_name) in columns.iter().enumerate() {
        // Expression key parts are masked with an empty slot by the
        // callers; their width is not part of MySQL's 3072 accounting.
        if column_name.is_empty() {
            continue;
        }
        let Some(column) = table_columns
            .iter()
            .find(|existing| normalize_name(&existing.name) == normalize_name(column_name))
        else {
            continue; // existence already rejected by validate_columns
        };
        // A JSON column cannot be indexed directly (MySQL 3152): indexing is
        // only via a generated column / multi-valued expression on a JSON
        // path. Expression key parts are masked empty above, so this only
        // catches a bare `INDEX(json_col)` / UNIQUE / PRIMARY KEY. Silently
        // building a b-tree over the raw jsonb bytes was a no-standard index.
        if matches!(column.data_type, DataTypeKind::Json) {
            return Err(error(crate::error::catalog::ER_JSON_USED_AS_KEY,
                format!(
                    "JSON column '{column_name}' supports indexing only via generated columns on a specified JSON path."
                ),
            ));
        }
        let prefix_len = prefix_lengths.get(pos).copied().flatten();
        if prefix_len.is_none()
            && matches!(
                column.data_type,
                DataTypeKind::Text { .. } | DataTypeKind::Blob { .. }
            )
        {
            if !lob_full_column_index_allowed() {
                return Err(error(crate::error::catalog::ER_BLOB_KEY_WITHOUT_LENGTH,
                    format!(
                        "BLOB/TEXT column '{column_name}' used in key specification without a key length"
                    ),
                ));
            }
            continue;
        }
        total_bytes += index_key_part_max_bytes(column, prefix_len);
    }
    if total_bytes > MAX_INDEX_KEY_BYTES {
        return Err(error(
            crate::error::catalog::ER_TOO_LONG_KEY,
            format!("Specified key was too long; max key length is {MAX_INDEX_KEY_BYTES} bytes"),
        ));
    }
    Ok(())
}

fn enforce_key_columns_not_null(
    columns: &[String],
    table_columns: &mut [ColumnSchema],
) -> Result<(), QueryError> {
    for column in columns {
        let Some(schema) = table_columns
            .iter_mut()
            .find(|existing| normalize_name(&existing.name) == normalize_name(column))
        else {
            return Err(error(
                crate::error::catalog::ER_KEY_COLUMN_DOES_NOT_EXITS,
                format!("key column '{column}' does not exist"),
            ));
        };
        schema.nullable = false;
    }
    Ok(())
}

fn validate_spatial_columns_not_null(
    columns: &[String],
    table_columns: &[ColumnSchema],
) -> Result<(), QueryError> {
    for column in columns {
        let Some(schema) = table_columns
            .iter()
            .find(|existing| normalize_name(&existing.name) == normalize_name(column))
        else {
            return Err(error(
                crate::error::catalog::ER_KEY_COLUMN_DOES_NOT_EXITS,
                format!("key column '{column}' does not exist"),
            ));
        };
        if schema.nullable {
            return Err(error(
                crate::error::catalog::ER_SPATIAL_CANT_HAVE_NULL,
                format!(
                    "All parts of a SPATIAL index must be NOT NULL; column '{column}' is nullable"
                ),
            ));
        }
    }
    Ok(())
}

fn enforce_existing_primary_key_not_null(
    db: &Database,
    table: &TableSchema,
    columns: &[String],
) -> Result<(), QueryError> {
    let column_indexes = columns
        .iter()
        .map(|column| table.column_index(column))
        .collect::<Result<Vec<_>, _>>()?;
    for row in db.scan_rows(
        &table.qualified_name(),
        TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
    )? {
        if column_indexes
            .iter()
            .any(|idx| matches!(row.values.get(*idx), Some(Value::Null)))
        {
            return Err(error(
                crate::error::catalog::ER_INVALID_USE_OF_NULL,
                "invalid use of NULL value for primary key",
            ));
        }
    }
    Ok(())
}

fn validate_existing_unique_index_values(
    db: &Database,
    table: &TableSchema,
    index: &IndexSchema,
) -> Result<(), QueryError> {
    let txn = TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted);
    // Functional indexes need the expression evaluator's full-row context;
    // retain the authoritative generic path for them. Plain indexes can scan
    // only their key columns and avoid materialising every wide heap row.
    if index.has_expression_parts() {
        let mut seen = crate::util::fast_hash::ah_hash_set();
        for row in db.scan_rows(&table.qualified_name(), txn)? {
            let key = table.index_key(index, &row.values)?;
            if key.0.iter().any(|part| matches!(part, KeyPart::Null)) {
                if index.primary {
                    return Err(error(
                        crate::error::catalog::ER_INVALID_USE_OF_NULL,
                        "invalid use of NULL value for primary key",
                    ));
                }
                continue;
            }
            if !seen.insert(key.clone()) {
                return Err(duplicate_entry_error(table, index, &key));
            }
        }
        return Ok(());
    }
    let column_indexes = index
        .columns
        .iter()
        .map(|column| table.column_index(column))
        .collect::<Result<Vec<_>, _>>()?;
    let mut seen = crate::util::fast_hash::ah_hash_set();
    db.scan_rows_columns_streaming(&table.qualified_name(), txn, &column_indexes, |values| {
        let mut parts = Vec::with_capacity(values.len());
        for (pos, (value, &column_index)) in values.iter().zip(column_indexes.iter()).enumerate() {
            if matches!(value, Value::Null) {
                if index.primary {
                    return Err(error(
                        crate::error::catalog::ER_INVALID_USE_OF_NULL,
                        "invalid use of NULL value for primary key",
                    ));
                }
                return Ok(());
            }
            let column = &table.columns[column_index];
            let part = KeyPart::from_column_value_with_collation(
                value,
                &column.data_type,
                column.collation.as_deref(),
            );
            parts.push(index.apply_prefix_to_key_part(pos, part));
        }
        let key = IndexKey::from(parts);
        if !seen.insert(key.clone()) {
            return Err(duplicate_entry_error(table, index, &key));
        }
        Ok(())
    })?;
    Ok(())
}

fn literal_default_value(expr: &Expr) -> Result<Value, QueryError> {
    match expr {
        Expr::Literal(Literal::Null) => Ok(Value::Null),
        Expr::Literal(Literal::Bool(value)) => Ok(Value::Int(*value as i64)),
        Expr::Literal(Literal::Int(value)) => Ok(Value::Int(*value)),
        Expr::Literal(Literal::UInt(value)) => Ok(Value::UInt(*value)),
        Expr::Literal(Literal::Float(value)) => Ok(Value::Double(*value)),
        Expr::Literal(Literal::Decimal(value)) => Ok(Value::Decimal(value.clone())),
        Expr::Literal(Literal::String(value)) => Ok(Value::String(value.clone())),
        // Bit / hex literals (`b'1010'`, `0xAA`, `X'CAFE'`) are valid literal
        // defaults — MySQL allows `BIT(4) DEFAULT b'1010'` and
        // `BINARY(2) DEFAULT 0xCAFE`. They carry decoded bytes; the column's
        // coercion (e.g. the BIT arm) interprets them at store time.
        Expr::Literal(Literal::BitBytes(value)) | Expr::Literal(Literal::HexBytes(value)) => {
            Ok(Value::Bytes(value.clone()))
        }
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => match literal_default_value(expr)? {
            Value::Int(value) => Ok(Value::Int(-value)),
            Value::UInt(value) if value == i64::MAX as u64 + 1 => Ok(Value::Int(i64::MIN)),
            Value::UInt(value) if value <= i64::MAX as u64 => Ok(Value::Int(-(value as i64))),
            Value::UInt(value) => Ok(Value::Decimal(format!("-{value}"))),
            Value::Decimal(value) => Ok(Value::Decimal(
                if let Some(rest) = value.strip_prefix('-') {
                    rest.to_string()
                } else {
                    format!("-{value}")
                },
            )),
            Value::Double(value) => Ok(Value::Double(-value)),
            Value::Float(value) => Ok(Value::Float(-value)),
            _ => Err(error(
                crate::error::catalog::ER_INVALID_DEFAULT,
                "invalid default value",
            )),
        },
        _ => Err(error(
            crate::error::catalog::ER_INVALID_DEFAULT,
            "only literal column default values are supported",
        )),
    }
}

fn is_current_timestamp_expr(expr: &Expr) -> bool {
    current_timestamp_expr_fsp(expr).is_some()
}

/// Recognise the `CURRENT_TIMESTAMP` family (CURRENT_TIMESTAMP / NOW /
/// LOCALTIME / LOCALTIMESTAMP, with an optional `(fsp)` argument in 0..=6).
/// Returns `Some(declared_fsp)` when the expression is a member —
/// `Some(None)` for the bare form, `Some(Some(n))` for `…(n)`.
fn current_timestamp_expr_fsp(expr: &Expr) -> Option<Option<u8>> {
    let is_family_name = |name: &str| {
        name.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
            || name.eq_ignore_ascii_case("NOW")
            || name.eq_ignore_ascii_case("LOCALTIME")
            || name.eq_ignore_ascii_case("LOCALTIMESTAMP")
    };
    match expr {
        Expr::Function { name, args, .. } if is_family_name(name) => match args.as_slice() {
            [] => Some(None),
            [Expr::Literal(crate::parser::Literal::Int(fsp))] if (0..=6).contains(fsp) => {
                Some(Some(*fsp as u8))
            }
            _ => None,
        },
        Expr::Identifier(name) if is_family_name(name) && !name.eq_ignore_ascii_case("NOW") => {
            Some(None)
        }
        _ => None,
    }
}

fn apply_legacy_first_timestamp_defaults(columns: &mut [ColumnSchema]) {
    if crate::engine::expr::session_explicit_defaults_for_timestamp() {
        return;
    }
    let Some(column) = columns
        .iter_mut()
        .find(|column| matches!(column.data_type, DataTypeKind::Timestamp { .. }))
    else {
        return;
    };
    if column.default.is_none() && column.default_expr.is_none() && column.on_update_expr.is_none()
    {
        let fsp = match column.data_type {
            DataTypeKind::Timestamp { fsp } => fsp,
            _ => 0,
        };
        let current = if fsp == 0 {
            "CURRENT_TIMESTAMP".to_string()
        } else {
            format!("CURRENT_TIMESTAMP({fsp})")
        };
        column.nullable = false;
        column.default_expr = Some(current.clone());
        column.on_update_expr = Some(current);
    }
}

fn column_def_to_schema(column: ColumnDef) -> Result<ColumnSchema, QueryError> {
    validate_mysql_identifier(&column.name)?;
    if column.generated.is_some() {
        if column.default.is_some() {
            return Err(error(
                crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                "generated column cannot have a DEFAULT value",
            ));
        }
        if column.on_update.is_some() {
            return Err(error(
                crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                "generated column cannot have an ON UPDATE clause",
            ));
        }
        if column.auto_increment {
            return Err(error(
                crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                "generated column cannot be AUTO_INCREMENT",
            ));
        }
    }
    if column.default.as_ref().is_some_and(|expr| crate::parser::visit::any_expr(expr, |node| matches!(node, crate::parser::Expr::Function {name,..} if crate::engine::http::is_function(name) || (name != crate::parser::DEFAULT_EXPR_SENTINEL && !crate::engine::cbo::function_cost::is_known_builtin_function(name))))) {
        return Err(crate::engine::http::error("ER_HTTP_UNSUPPORTED_CONTEXT", "HTTP calls are forbidden in column defaults"));
    }
    if let Some(crate::parser::Expr::Function { name, args, .. }) = column.default.as_ref() {
        if name == crate::parser::DEFAULT_EXPR_SENTINEL
            && args.len() == 1
            && crate::storage::schema::expr_contains_function_named(&args[0], "LOAD_FILE")
        {
            return Err(error(
                crate::error::catalog::ER_DEFAULT_VAL_GENERATED_NAMED_FUNCTION_IS_NOT_ALLOWED,
                format!(
                    "Default value expression of column '{}' contains a disallowed function: load_file.",
                    column.name
                ),
            ));
        }
    }
    // MySQL 8.0.13+ `DEFAULT (<expr>)` is wrapped by the parser as
    // `Expr::Function { name: __DEFAULT_EXPR, args: [inner] }`. Unwrap it and
    // stash the inner expression's textual SQL on `default_expr` so INSERT /
    // ALTER ADD COLUMN backfill can re-parse and evaluate at runtime. Plain
    // `DEFAULT CURRENT_TIMESTAMP` (no parens) keeps the legacy bare-string
    // sentinel.
    let default_expr = column
        .default
        .as_ref()
        .map(|expr| {
            if let crate::parser::Expr::Function { name, args, .. } = expr {
                if name == crate::parser::DEFAULT_EXPR_SENTINEL && args.len() == 1 {
                    return Ok(Some(encode_catalog_expr(&args[0])?));
                }
            }
            // Bare `DEFAULT CURRENT_TIMESTAMP[(fsp)]` follows the same MySQL
            // rule as ON UPDATE: TIMESTAMP / DATETIME columns only, exact fsp
            // match (ER_INVALID_DEFAULT 1067 otherwise).
            let Some(declared_fsp) = current_timestamp_expr_fsp(expr) else {
                return Ok(None);
            };
            let invalid = || {
                error(
                    crate::error::catalog::ER_INVALID_DEFAULT,
                    format!("Invalid default value for '{}'", column.name),
                )
            };
            let column_fsp = match &column.data_type {
                crate::parser::DataType::Timestamp { fsp }
                | crate::parser::DataType::DateTime { fsp } => *fsp,
                _ => return Err(invalid()),
            };
            if declared_fsp.unwrap_or(0) != column_fsp {
                return Err(invalid());
            }
            if column_fsp == 0 {
                Ok(Some("CURRENT_TIMESTAMP".to_string()))
            } else {
                Ok(Some(format!("CURRENT_TIMESTAMP({column_fsp})")))
            }
        })
        .transpose()?
        .flatten();
    // MySQL only allows `ON UPDATE CURRENT_TIMESTAMP[(fsp)]` (plus the NOW /
    // LOCALTIME / LOCALTIMESTAMP synonyms) on TIMESTAMP / DATETIME columns,
    // and the declared fsp must match the column's. Anything else is
    // ER_INVALID_ON_UPDATE (1294) — never silently dropped.
    let on_update_expr = column
        .on_update
        .as_ref()
        .map(|expr| {
            let invalid = || {
                error(
                    crate::error::catalog::ER_INVALID_ON_UPDATE,
                    format!("Invalid ON UPDATE clause for '{}' column", column.name),
                )
            };
            let declared_fsp = current_timestamp_expr_fsp(expr).ok_or_else(invalid)?;
            let column_fsp = match &column.data_type {
                crate::parser::DataType::Timestamp { fsp }
                | crate::parser::DataType::DateTime { fsp } => *fsp,
                _ => return Err(invalid()),
            };
            // MySQL requires an exact fsp match: bare CURRENT_TIMESTAMP is
            // fsp 0, so TIMESTAMP(6) needs an explicit CURRENT_TIMESTAMP(6).
            if declared_fsp.unwrap_or(0) != column_fsp {
                return Err(invalid());
            }
            if column_fsp == 0 {
                Ok("CURRENT_TIMESTAMP".to_string())
            } else {
                Ok(format!("CURRENT_TIMESTAMP({column_fsp})"))
            }
        })
        .transpose()?;
    if column
        .generated
        .as_ref()
        .is_some_and(|g| crate::planner::expr_has_window_function(&g.expr))
    {
        return Err(error(
            crate::error::catalog::ER_WINDOW_INVALID_WINDOW_FUNC_USE,
            "window function is not allowed in a generated column",
        ));
    }
    let generated_expr = column
        .generated
        .as_ref()
        .map(|generated| encode_catalog_expr(&generated.expr))
        .transpose()?;
    let generated_stored = column
        .generated
        .as_ref()
        .is_some_and(|generated| generated.stored);
    let mut default = if default_expr.is_some() {
        None
    } else {
        column
            .default
            .map(|expr| literal_default_value(&expr))
            .transpose()?
    };
    let mut data_type: DataTypeKind = column.data_type.into();
    if let DataTypeKind::Geometry {
        srid: Some(srid), ..
    } = &data_type
    {
        crate::engine::geo::srs::require(*srid, "column definition")?;
    }
    // MySQL strips trailing U+0020 from ENUM/SET declarations before
    // duplicate/default validation and catalog storage (`SET(' ')` is
    // displayed as `SET('')`). Keep this normalization at the shared
    // CREATE/ALTER boundary.
    match &mut data_type {
        DataTypeKind::Enum { values } | DataTypeKind::Set { values } => {
            for value in values {
                let trimmed_len = value.trim_end_matches(' ').len();
                value.truncate(trimmed_len);
            }
            // Hex ENUM/SET members/defaults arrive as their original bytes.
            // The parser normalizes member bytes one-to-one; keep literal
            // defaults in the same representation so membership validation
            // and later row materialization agree.
            if let Some(value) = default.take() {
                default = Some(match value {
                    Value::Bytes(bytes) => {
                        Value::String(bytes.into_iter().map(char::from).collect::<String>())
                    }
                    other => other,
                });
            }
        }
        _ => {}
    }
    // Resolve metadata before ENUM/SET validation: duplicate membership and
    // DEFAULT lookup are collation-sensitive (`latin1_bin` permits 'a' and
    // 'A', while a case-insensitive collation does not).
    let charset = column
        .charset
        .as_deref()
        .map(|charset| crate::charset::resolve_charset(charset).map(|info| info.name.to_string()))
        .transpose()?;
    let binary_attribute =
        column.collation.as_deref() == Some(crate::parser::BINARY_ATTRIBUTE_COLLATION);
    let explicit_collation = if binary_attribute {
        None
    } else {
        column
            .collation
            .as_deref()
            .map(|collation| {
                crate::charset::resolve_collation(collation).map(|info| info.name.to_string())
            })
            .transpose()?
    };
    let collation = match (explicit_collation, charset.as_deref()) {
        (Some(collation), _) => Some(collation),
        (None, _) if binary_attribute => {
            Some(crate::parser::BINARY_ATTRIBUTE_COLLATION.to_string())
        }
        (None, Some(charset)) => {
            Some(crate::charset::default_collation_for_charset(charset)?.to_string())
        }
        (None, None) => None,
    };
    if let (Some(charset), Some(collation)) = (&charset, &collation) {
        crate::charset::validate_charset_collation_pair(charset, collation)?;
    }
    // MySQL forbids a *literal* DEFAULT on BLOB/TEXT/GEOMETRY/JSON columns;
    // 8.0.13+ allows only the expression form `DEFAULT (<expr>)`. In strict
    // mode this is ER 1101. In explicitly non-strict mode MySQL creates the
    // column without the invalid default and emits warning 1101. A bare
    // DEFAULT NULL remains valid. CREATE and ADD/MODIFY COLUMN all share this
    // boundary, so the mode policy and catalog normalization stay uniform.
    if default_expr.is_none() {
        if let Some(value) = &default {
            if !matches!(value, Value::Null)
                && matches!(
                    data_type,
                    DataTypeKind::Blob { .. }
                        | DataTypeKind::Text { .. }
                        | DataTypeKind::Geometry { .. }
                        | DataTypeKind::Json
                )
            {
                let message = format!(
                    "BLOB, TEXT, GEOMETRY or JSON column '{}' can't have a default value",
                    column.name
                );
                let legacy_empty_default = matches!(
                    value,
                    Value::String(text) if text.is_empty()
                ) || matches!(
                    value,
                    Value::SmallString(bytes) if bytes.is_empty()
                ) || matches!(
                    value,
                    Value::Bytes(bytes) if bytes.is_empty()
                );
                if coercion_strict() || !legacy_empty_default {
                    return Err(error(
                        crate::error::catalog::ER_BLOB_CANT_HAVE_DEFAULT,
                        message,
                    ));
                }
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::ER_BLOB_CANT_HAVE_DEFAULT,
                    message,
                );
                default = None;
            }
        }
    }
    // MySQL restricts BIT(M) to 1..=64. Reject a wider width here — the
    // single source covering CREATE, ADD/MODIFY/CHANGE COLUMN — with error
    // 1439, before it can reach the row codec (which assumes ceil(M/8) <= 8
    // and would otherwise panic). BIT(0) is already normalized to BIT(1) by
    // the DataType -> DataTypeKind conversion.
    if let DataTypeKind::Bit { len: Some(n) } = &data_type {
        if *n > 64 {
            return Err(error(
                crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
                format!(
                    "Display width out of range for column '{}' (max = 64)",
                    column.name
                ),
            ));
        }
    }
    // MySQL's fixed-width CHAR/BINARY types are capped at 255 elements. Keep
    // this validation at the common column-definition boundary so CREATE and
    // every ALTER variant return the same ER_TOO_BIG_FIELDLENGTH.
    let declared_len = match &data_type {
        DataTypeKind::Char { len } | DataTypeKind::Binary { len } => {
            len.map(|length| (length, 255usize))
        }
        // An oversized VARCHAR/VARBINARY without a default may be promoted
        // to a LOB by MySQL's DDL layer. A literal default makes that
        // promotion illegal because the resulting LOB cannot retain it.
        DataTypeKind::VarChar { len } | DataTypeKind::VarBinary { len }
            if default_expr.is_none()
                && default
                    .as_ref()
                    .is_some_and(|value| !matches!(value, Value::Null)) =>
        {
            len.filter(|length| *length > 65_535)
                .map(|length| (length, 65_535usize))
        }
        _ => None,
    };
    if let Some((length, maximum)) = declared_len {
        if length > maximum {
            return Err(error(crate::error::catalog::ER_TOO_BIG_FIELDLENGTH,
                format!(
                    "Column length too big for column '{}' (max = {maximum}); use BLOB or TEXT instead",
                    column.name
                ),
            ));
        }
    }
    // In non-strict DDL MySQL promotes a variable-length declaration whose
    // maximum encoded width no longer fits the 16-bit VARCHAR/VARBINARY
    // length domain. The promoted LOB tier is chosen from the byte width, and
    // the catalog/protocol metadata must retain that tier (MEDIUM/LONG)
    // instead of keeping an impossible VARCHAR(N).
    let lob_tier = |bytes: u64| {
        use crate::storage::schema::{DEFAULT_LOB_MAX, LONG_LOB_MAX, MEDIUM_LOB_MAX, TINY_LOB_MAX};
        match bytes {
            0..=255 => TINY_LOB_MAX,
            256..=65_535 => DEFAULT_LOB_MAX,
            65_536..=16_777_215 => MEDIUM_LOB_MAX,
            _ => LONG_LOB_MAX,
        }
    };
    let promoted = match &data_type {
        DataTypeKind::VarBinary { len: Some(length) } if *length > 65_535 => Some((
            DataTypeKind::Blob {
                max_len: lob_tier(*length as u64),
            },
            "VARBINARY",
            "BLOB",
        )),
        DataTypeKind::VarChar { len: Some(length) } => {
            let max_bytes_per_char = charset
                .as_deref()
                .and_then(|name| crate::charset::resolve_charset(name).ok())
                .map(|info| u64::from(info.max_len));
            max_bytes_per_char.and_then(|max_bytes_per_char| {
                let bytes = (*length as u64).saturating_mul(max_bytes_per_char);
                (bytes > 65_535).then(|| {
                    (
                        DataTypeKind::Text {
                            max_len: lob_tier(bytes),
                        },
                        "VARCHAR",
                        "TEXT",
                    )
                })
            })
        }
        _ => None,
    };
    if let Some((promoted_type, from, to)) = promoted {
        if coercion_strict() {
            return Err(error(
                crate::error::catalog::ER_TOO_BIG_FIELDLENGTH,
                format!(
                    "Column length too big for column '{}' (max = 65535); use BLOB or TEXT instead",
                    column.name
                ),
            ));
        }
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Note,
            crate::error::catalog::ER_AUTO_CONVERT,
            format!("Converting column '{}' from {from} to {to}", column.name),
        );
        data_type = promoted_type;
    }
    // ENUM / SET member list validation — MySQL 8.0 rejects at DDL
    // time; without this check we'd accept `ENUM('a','a')`, `SET('a,b')`,
    // 66 000-member ENUMs, or 128-member SETs and only surface trouble
    // at INSERT / on-disk encode time.
    match &data_type {
        DataTypeKind::Enum { values } => {
            validate_enum_set_members(&column.name, values, false, collation.as_deref())?;
        }
        DataTypeKind::Set { values } => {
            validate_enum_set_members(&column.name, values, true, collation.as_deref())?;
        }
        _ => {}
    }
    // ENUM / SET literal DEFAULT: must be a declared member (or NULL for
    // nullable columns). Numeric DEFAULT is legal for ENUM but not SET.
    // Failing here mirrors MySQL's ER_INVALID_DEFAULT(1067) at DDL.
    if default_expr.is_none() {
        if let Some(literal) = &default {
            if !matches!(literal, Value::Null) {
                if let Some(err) =
                    enum_set_default_error(&column.name, &data_type, literal, collation.as_deref())
                {
                    return Err(err);
                }
            }
        }
    }
    // Validate and canonicalize literal defaults at DDL time through the
    // same type-coercion kernel used by INSERT/UPDATE. Invalid temporal and
    // numeric defaults are definition errors (1067), not latent failures on
    // the first row that happens to use DEFAULT.
    if default_expr.is_none() {
        if let Some(literal) = default.take() {
            if matches!(literal, Value::Null) {
                default = Some(Value::Null);
            } else if matches!(data_type, DataTypeKind::Enum { .. })
                && matches!(literal, Value::Int(0) | Value::UInt(0))
            {
                // ENUM/SET defaults have already gone through their dedicated
                // member/ordinal validator above. Canonicalize an ENUM
                // DEFAULT 0 to the internal stored sentinel now: applying a
                // schema default is not a new invalid user conversion and
                // must not emit 1265 for every subsequently inserted row.
                default = Some(enum_zero_value());
            } else if matches!(
                data_type,
                DataTypeKind::Enum { .. } | DataTypeKind::Set { .. }
            ) {
                default = Some(literal);
            } else {
                default = Some(
                    coerce_value_with_unsigned(literal, &data_type, column.unsigned, &column.name)
                        .map_err(|_| {
                            error(
                                crate::error::catalog::ER_INVALID_DEFAULT,
                                format!("Invalid default value for '{}'", column.name),
                            )
                        })?,
                );
            }
        }
    }
    Ok(ColumnSchema {
        not_null_from_scn: 0,
        name: column.name,
        data_type,
        nullable: column.nullable,
        unsigned: column.unsigned,
        zerofill: column.zerofill,
        invisible: column.invisible,
        auto_increment: column.auto_increment,
        default,
        default_expr,
        read_backfill_default: None,
        on_update_expr,
        // Identity / online-DDL fields are populated when the column is
        // attached to a TableSchema (CREATE TABLE / ALTER ADD COLUMN);
        // leave them at defaults here. `TableSchema::build_column_index`
        // assigns column_id and the legacy migration assigns physical /
        // logical positions.
        column_id: 0,
        state: crate::storage::ColumnState::Public,
        physical_pos: 0,
        logical_pos: 0,
        ordinal_position: 0,
        charset,
        collation,
        comment: String::new(),
        generated_expr,
        generated_stored,
        generated_column_names: Vec::new(),
        generated_expr_ast: None,
        generated_program: None,
    })
}

/// Persist only expressions accepted by the scalar SQL encoder.
fn encode_catalog_expr(expr: &Expr) -> Result<String, QueryError> {
    crate::parser::expr_to_catalog_sql(expr)
        .map_err(|err| error(crate::error::catalog::ER_NOT_SUPPORTED_YET, err.to_string()))
}
