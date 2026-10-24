use crate::charset::encode_file_text as encode_text;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use crate::engine::api::{push_statement_warning, QueryResponse, SessionContext};
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::engine::session_registry::SessionRegistry;
use crate::parser::{QueryExpr, SelectIntoFileKind, SelectIntoFileOptions, SelectIntoFilePosition};
use crate::storage::{error, Database};
use crate::types::{Column, ColumnType, QueryError, Value};

use super::execute_statement;

static ACTIVE_WRITER_RESERVED_BYTES: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn active_writer_reserved_bytes() -> usize {
    ACTIVE_WRITER_RESERVED_BYTES.load(Ordering::Relaxed)
}

pub(super) fn execute_select_into_file(
    db: &Arc<Database>,
    session: &mut SessionContext,
    inner: QueryExpr,
    options: SelectIntoFileOptions,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let encoded = EncodedOptions::new(&options)?;
    let hint_settings = SelectIntoHintSettings::from_statement(&inner)?;
    let path = resolve_export_path(db, session, &options.file_path)?;
    enforce_secure_file_priv(&path)?;

    let file = create_export_file(&path)?;
    let buffer_size = usize::try_from(
        hint_settings
            .buffer_size
            .unwrap_or(session.sysvars.select_into_buffer_size),
    )
    .unwrap_or(usize::MAX)
    .clamp(8_192, 2_147_479_552);
    let mut buffer_charge = match ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::StorageScratch,
        buffer_size,
    ) {
        Ok(charge) => charge,
        Err(err) => {
            drop(file);
            let _ = std::fs::remove_file(&path);
            return Err(err);
        }
    };
    let mut buffer = Vec::new();
    if buffer.try_reserve_exact(buffer_size).is_err() {
        drop(buffer_charge);
        drop(file);
        let _ = std::fs::remove_file(&path);
        return Err(crate::engine::executor::mem_budget::allocation_error(
            format!("Out of memory allocating SELECT INTO buffer ({buffer_size} bytes)"),
        ));
    }
    let actual_capacity = buffer.capacity();
    if actual_capacity > buffer_size {
        if let Err(err) = buffer_charge.try_grow(actual_capacity - buffer_size) {
            drop(buffer_charge);
            drop(file);
            let _ = std::fs::remove_file(&path);
            return Err(err);
        }
    } else if actual_capacity < buffer_size {
        buffer_charge.shrink(buffer_size - actual_capacity);
    }
    ACTIVE_WRITER_RESERVED_BYTES.fetch_add(actual_capacity, Ordering::Relaxed);
    let writer_reservation = WriterReservation {
        charge: buffer_charge,
        bytes: actual_capacity,
    };
    let mut sink = ExportSink {
        file,
        buffer,
        _writer_reservation: writer_reservation,
        sync_enabled: hint_settings
            .disk_sync
            .unwrap_or(session.sysvars.select_into_disk_sync),
        sync_delay: Duration::from_millis(
            hint_settings
                .disk_sync_delay_ms
                .unwrap_or(session.sysvars.select_into_disk_sync_delay_ms),
        ),
        path: path.clone(),
    };

    if matches!(
        options.position,
        SelectIntoFilePosition::AfterSelectList | SelectIntoFilePosition::BeforeLockingClause
    ) {
        push_statement_warning(
            crate::error::ConditionLevel::Warning,
             crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
            "The SELECT ... INTO OUTFILE/DUMPFILE position after the select list is deprecated; place INTO at the end of the statement",
        );
    }

    // No export bytes have been written yet. A preparation/execution error
    // must not reserve this pathname and block a corrected retry.
    let response = match execute_statement(db, session, inner.into_statement(), registry) {
        Ok(response) => response,
        Err(err) => {
            drop(sink);
            let _ = std::fs::remove_file(&path);
            return Err(err);
        }
    };
    let affected_rows = match response {
        QueryResponse::ResultSet(result) => export_rows(
            &mut sink,
            &result.columns,
            result.rows.into_iter().map(Ok),
            &options,
            &encoded,
        )?,
        QueryResponse::StreamingResultSet(mut result) => {
            let columns = result.columns;
            export_rows(
                &mut sink,
                &columns,
                std::iter::from_fn(move || match result.cursor.next_row() {
                    Ok(Some(row)) => Some(Ok(row)),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                }),
                &options,
                &encoded,
            )?
        }
        other => {
            return Err(error(
                crate::error::catalog::ER_PARSE_ERROR,
                format!(
                    "SELECT INTO OUTFILE/DUMPFILE requires a result set, got {}",
                    response_kind(&other)
                ),
            ))
        }
    };
    sink.finish()?;
    Ok(QueryResponse::OkAffected { affected_rows })
}

#[derive(Default)]
struct SelectIntoHintSettings {
    buffer_size: Option<u64>,
    disk_sync: Option<bool>,
    disk_sync_delay_ms: Option<u64>,
}

impl SelectIntoHintSettings {
    fn from_statement(statement: &QueryExpr) -> Result<Self, QueryError> {
        let mut settings = Self::default();
        collect_set_var_hints(statement, &mut settings)?;
        Ok(settings)
    }
}

fn collect_set_var_hints<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
    settings: &mut SelectIntoHintSettings,
) -> Result<(), QueryError> {
    let statement = statement.into();
    match statement {
        crate::parser::QueryRef::Select(select) => {
            for hint in &select.query_hints {
                let crate::parser::QueryHint::SetVar { name, value } = hint else {
                    continue;
                };
                match name.as_str() {
                    "select_into_buffer_size" => {
                        let parsed = value.parse::<u64>().map_err(|_| {
                            error(crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                                format!(
                                    "Variable 'select_into_buffer_size' can't be set to the value of '{value}'"
                                ),
                            )
                        })?;
                        if !(8_192..=2_147_479_552).contains(&parsed) {
                            return Err(error(crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                                format!(
                                    "Variable 'select_into_buffer_size' can't be set to the value of '{value}'"
                                ),
                            ));
                        }
                        settings.buffer_size = Some(parsed / 4096 * 4096);
                    }
                    "select_into_disk_sync" => {
                        settings.disk_sync = Some(match value.to_ascii_uppercase().as_str() {
                            "1" | "ON" | "TRUE" => true,
                            "0" | "OFF" | "FALSE" => false,
                            _ => {
                                return Err(error(crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                                    format!(
                                        "Variable 'select_into_disk_sync' can't be set to the value of '{value}'"
                                    ),
                                ))
                            }
                        });
                    }
                    "select_into_disk_sync_delay" => {
                        let parsed = value.parse::<u64>().map_err(|_| {
                            error(crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                                format!(
                                    "Variable 'select_into_disk_sync_delay' can't be set to the value of '{value}'"
                                ),
                            )
                        })?;
                        if parsed > 31_536_000 {
                            return Err(error(crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                                format!(
                                    "Variable 'select_into_disk_sync_delay' can't be set to the value of '{value}'"
                                ),
                            ));
                        }
                        settings.disk_sync_delay_ms = Some(parsed);
                    }
                    _ => {}
                }
            }
        }
        crate::parser::QueryRef::SetOperation(set) => {
            collect_set_var_hints(&set.left, settings)?;
            collect_set_var_hints(&set.right, settings)?;
        }
        crate::parser::QueryRef::WithCte(with) => collect_set_var_hints(&with.body, settings)?,
        _ => {}
    }
    Ok(())
}

fn response_kind(response: &QueryResponse) -> &'static str {
    match response {
        QueryResponse::Ok => "OK",
        QueryResponse::OkAffected { .. } => "OK affected",
        QueryResponse::ResultSet(_) => "result set",
        QueryResponse::SharedResultSet(_) => "result set",
        QueryResponse::StreamingResultSet(_) => "streaming result set",
        QueryResponse::MultiResult(_) => "multiple result sets",
    }
}

fn resolve_export_path(
    db: &Database,
    session: &SessionContext,
    file_name: &str,
) -> Result<PathBuf, QueryError> {
    let requested_buf = filesystem_path(file_name, &session.sysvars.character_set_filesystem)?;
    let requested = requested_buf.as_path();
    if requested.is_absolute() {
        return Ok(requested.to_path_buf());
    }

    let has_directory = requested.components().any(|component| {
        !matches!(component, Component::Normal(_) if requested.file_name() == Some(component.as_os_str()))
    });
    if has_directory {
        Ok(db.data_dir().join(requested))
    } else {
        if session.current_database.is_empty() {
            return Err(error(
                crate::error::catalog::ER_NO_DB_ERROR,
                "No database selected",
            ));
        }
        Ok(
            crate::storage::naming::schema_dir(db.data_dir(), &session.current_database)
                .join(requested),
        )
    }
}

#[cfg(unix)]
fn filesystem_path(file_name: &str, charset: &str) -> Result<PathBuf, QueryError> {
    use std::os::unix::ffi::OsStringExt;
    if is_binary_charset(charset) {
        Ok(PathBuf::from(file_name))
    } else {
        Ok(PathBuf::from(std::ffi::OsString::from_vec(encode_text(
            charset, file_name,
        )?)))
    }
}

#[cfg(not(unix))]
fn filesystem_path(file_name: &str, charset: &str) -> Result<PathBuf, QueryError> {
    // Windows paths cannot carry arbitrary byte sequences. Validate the
    // requested charset, then retain the Unicode path representation.
    let _ = encode_text(charset, file_name)?;
    Ok(PathBuf::from(file_name))
}

fn enforce_secure_file_priv(path: &Path) -> Result<(), QueryError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let canonical_parent = parent.canonicalize().map_err(|err| {
        error(
            crate::error::catalog::ER_CANT_CREATE_FILE,
            format!(
                "Can't create file '{}' (OS errno {})",
                path.display(),
                err.raw_os_error().unwrap_or(0)
            ),
        )
    })?;
    let configured = crate::engine::sysvar::builtin::query::global_secure_file_priv();
    if configured.eq_ignore_ascii_case("NULL") {
        return Err(error(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
            "The MySQL server is running with the --secure-file-priv option so it cannot execute this statement",
        ));
    }
    if configured.is_empty() {
        return Ok(());
    }
    let allowed = Path::new(&configured).canonicalize().map_err(|_| {
        error(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
            "The MySQL server is running with the --secure-file-priv option so it cannot execute this statement",
        )
    })?;
    if canonical_parent.starts_with(&allowed) {
        Ok(())
    } else {
        Err(error(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
            "The MySQL server is running with the --secure-file-priv option so it cannot execute this statement",
        ))
    }
}

fn create_export_file(path: &Path) -> Result<File, QueryError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o640);
    let file = options.open(path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::AlreadyExists {
            error(
                crate::error::catalog::ER_FILE_EXISTS_ERROR,
                format!("File '{}' already exists", path.display()),
            )
        } else {
            error(
                crate::error::catalog::ER_CANT_CREATE_FILE,
                format!(
                    "Can't create file '{}' (OS errno {})",
                    path.display(),
                    err.raw_os_error().unwrap_or(0)
                ),
            )
        }
    })?;
    #[cfg(unix)]
    if let Err(err) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o640)) {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(error(
            crate::error::catalog::ER_CANT_CREATE_FILE,
            format!(
                "Can't create file '{}' (OS errno {})",
                path.display(),
                err.raw_os_error().unwrap_or(0)
            ),
        ));
    }
    Ok(file)
}

struct EncodedOptions {
    field_terminated_by: Vec<u8>,
    enclosure: Vec<u8>,
    escape: Option<u8>,
    line_terminated_by: Vec<u8>,
    line_starting_by: Vec<u8>,
    charset: String,
    _charge: ControlledMemoryCharge,
}

impl EncodedOptions {
    fn new(options: &SelectIntoFileOptions) -> Result<Self, QueryError> {
        let charset = options
            .charset
            .clone()
            .unwrap_or_else(|| "binary".to_string());
        // Validate even when every selected row happens to be NULL.
        encode_text(&charset, "")?;
        let enclosure = options
            .field_enclosed_by
            .map(|ch| encode_text(&charset, &ch.to_string()))
            .transpose()?
            .unwrap_or_default();
        let escape_bytes = options
            .field_escaped_by
            .map(|ch| encode_text(&charset, &ch.to_string()))
            .transpose()?
            .unwrap_or_default();
        if enclosure.len() > 1 || escape_bytes.len() > 1 {
            push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED,
                "Non-ASCII ENCLOSED BY or ESCAPED BY character uses its first encoded byte",
            );
        }
        let field_terminated_by = encode_text(&charset, &options.field_terminated_by)?;
        let line_terminated_by = if options.line_terminated_by.is_empty() {
            field_terminated_by.clone()
        } else {
            encode_text(&charset, &options.line_terminated_by)?
        };
        if field_terminated_by
            .first()
            .is_some_and(|byte| !byte.is_ascii())
            || line_terminated_by
                .first()
                .is_some_and(|byte| !byte.is_ascii())
        {
            push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED,
                "Non-ASCII separator arguments are not fully portable in INTO OUTFILE",
            );
        }
        let line_starting_by = encode_text(&charset, &options.line_starting_by)?;
        let retained_capacity = field_terminated_by
            .capacity()
            .saturating_add(enclosure.capacity())
            .saturating_add(line_terminated_by.capacity())
            .saturating_add(line_starting_by.capacity())
            .saturating_add(charset.capacity());
        let charge = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::StorageScratch,
            retained_capacity,
        )?;
        Ok(Self {
            field_terminated_by,
            enclosure,
            escape: escape_bytes.first().copied(),
            line_terminated_by,
            line_starting_by,
            charset,
            _charge: charge,
        })
    }
}

fn export_rows<I>(
    sink: &mut ExportSink,
    columns: &[Column],
    rows: I,
    options: &SelectIntoFileOptions,
    encoded: &EncodedOptions,
) -> Result<u64, QueryError>
where
    I: IntoIterator<Item = Result<Vec<Value>, QueryError>>,
{
    let mut count = 0u64;
    for row in rows {
        let row = row?;
        if options.kind == SelectIntoFileKind::Dumpfile && count != 0 {
            return Err(error(
                crate::error::catalog::ER_TOO_MANY_ROWS,
                "Result consisted of more than one row",
            ));
        }
        if options.kind == SelectIntoFileKind::Dumpfile {
            write_dumpfile_row(sink, columns, &row)?;
        } else {
            write_outfile_row(sink, columns, &row, options, encoded)?;
        }
        count = count.saturating_add(1);
    }
    Ok(count)
}

fn write_dumpfile_row(
    sink: &mut ExportSink,
    columns: &[Column],
    row: &[Value],
) -> Result<(), QueryError> {
    for (index, value) in row.iter().enumerate() {
        if matches!(value, Value::Null) {
            sink.write_all(&[0])?;
        } else {
            with_value_bytes(value, columns.get(index), "binary", |bytes| {
                sink.write_all(bytes)
            })?;
        }
    }
    Ok(())
}

fn write_outfile_row(
    sink: &mut ExportSink,
    columns: &[Column],
    row: &[Value],
    options: &SelectIntoFileOptions,
    encoded: &EncodedOptions,
) -> Result<(), QueryError> {
    sink.write_all(&encoded.line_starting_by)?;
    let fixed_width = encoded.field_terminated_by.is_empty()
        && encoded.line_terminated_by.is_empty()
        && encoded.enclosure.is_empty();
    for (index, value) in row.iter().enumerate() {
        if index != 0 {
            sink.write_all(&encoded.field_terminated_by)?;
        }
        let width = fixed_width
            .then(|| fixed_field_width(columns.get(index), value))
            .unwrap_or(0);
        if matches!(value, Value::Null) {
            if fixed_width {
                write_spaces(sink, width)?;
            } else {
                match encoded.escape {
                    Some(escape) => sink.write_all(&[escape, b'N'])?,
                    None => sink.write_all(b"NULL")?,
                }
            }
            continue;
        }

        let enclosed = !encoded.enclosure.is_empty()
            && (!options.field_optionally_enclosed
                || columns
                    .get(index)
                    .is_none_or(|column| is_string_result(column.column_type)));
        if enclosed {
            sink.write_all(&encoded.enclosure)?;
        }
        let mut value_len = 0usize;
        with_value_bytes(value, columns.get(index), &encoded.charset, |bytes| {
            value_len = bytes.len();
            if fixed_width {
                write_escaped(sink, bytes, encoded, None)
            } else {
                write_escaped(
                    sink,
                    bytes,
                    encoded,
                    enclosed
                        .then_some(encoded.enclosure.first().copied())
                        .flatten(),
                )
            }
        })?;
        if fixed_width {
            write_spaces(sink, width.saturating_sub(value_len))?;
        }
        if enclosed {
            sink.write_all(&encoded.enclosure)?;
        }
    }
    sink.write_all(&encoded.line_terminated_by)
}

fn fixed_field_width(column: Option<&Column>, value: &Value) -> usize {
    if let Some(length) = column.and_then(|column| column.source.length) {
        // ColumnSource.length is the protocol maximum in bytes. Character
        // columns already multiplied their declared character width by the
        // charset max length when the source metadata was built; applying the
        // multiplier again here inflated utf8mb4 CHAR/VARCHAR fields 4×.
        return length as usize;
    }
    match column.map(|column| column.column_type) {
        Some(ColumnType::Tiny) => 4,
        Some(ColumnType::Short) => 6,
        Some(ColumnType::MediumInt) => 9,
        Some(ColumnType::Long) => 11,
        Some(ColumnType::LongLong) => 20,
        Some(ColumnType::Float) => 12,
        Some(ColumnType::Double) => 22,
        Some(ColumnType::Decimal { precision, scale }) => {
            usize::from(precision) + if scale > 0 { 2 } else { 1 }
        }
        Some(ColumnType::Date) => 10,
        Some(ColumnType::Time) => 10,
        Some(ColumnType::DateTime | ColumnType::Timestamp) => 19,
        Some(ColumnType::Year) => 4,
        Some(ColumnType::Bit { bits }) => usize::from(bits).div_ceil(8),
        _ => match value {
            Value::Bytes(bytes) | Value::Geometry(bytes) => bytes.len(),
            Value::String(text) | Value::Decimal(text) => text.len(),
            Value::SmallString(text) => text.len(),
            _ => value.to_display_string().len(),
        },
    }
}

fn write_spaces(sink: &mut ExportSink, mut count: usize) -> Result<(), QueryError> {
    const SPACES: [u8; 64] = [b' '; 64];
    while count != 0 {
        let chunk = count.min(SPACES.len());
        sink.write_all(&SPACES[..chunk])?;
        count -= chunk;
    }
    Ok(())
}

fn is_string_result(column_type: ColumnType) -> bool {
    !matches!(
        column_type,
        ColumnType::Tiny
            | ColumnType::Short
            | ColumnType::MediumInt
            | ColumnType::Long
            | ColumnType::LongLong
            | ColumnType::Float
            | ColumnType::Double
            | ColumnType::Decimal { .. }
            | ColumnType::Bit { .. }
    )
}

fn with_value_bytes<R>(
    value: &Value,
    column: Option<&Column>,
    charset: &str,
    consume: impl FnOnce(&[u8]) -> Result<R, QueryError>,
) -> Result<R, QueryError> {
    if let (Value::UInt(value), Some(column)) = (value, column) {
        if let ColumnType::Bit { bits } = column.column_type {
            let width = usize::from(bits).div_ceil(8);
            let raw = value.to_be_bytes();
            return consume(&raw[raw.len() - width..]);
        }
    }
    match value {
        Value::Int(value) => {
            let mut buffer = itoa::Buffer::new();
            consume(buffer.format(*value).as_bytes())
        }
        Value::UInt(value) => {
            let mut buffer = itoa::Buffer::new();
            consume(buffer.format(*value).as_bytes())
        }
        Value::Float(value) => {
            let mut buffer = ryu::Buffer::new();
            consume(buffer.format(*value).as_bytes())
        }
        Value::Double(value) => {
            let mut buffer = ryu::Buffer::new();
            consume(buffer.format(*value).as_bytes())
        }
        Value::DecimalCompact(magnitude, scale) => {
            let mut buffer = [0u8; 48];
            consume(crate::types::write_decimal_compact_canonical(
                *magnitude,
                *scale,
                &mut buffer,
            ))
        }
        Value::Decimal128(..) | Value::DecimalWide(..) => {
            let text = value.to_display_string();
            if is_binary_charset(charset) {
                consume(text.as_bytes())
            } else {
                let encoded = encode_text(charset, &text)?;
                consume_owned_bytes(encoded, consume)
            }
        }
        Value::Date(value) => {
            let mut buffer = [0u8; 32];
            consume(crate::types::write_date_canonical(*value, &mut buffer))
        }
        Value::Time(value) => {
            let mut buffer = [0u8; 32];
            consume(crate::types::write_time_canonical_with_fsp(
                *value,
                column.and_then(|column| column.source.decimals),
                &mut buffer,
            ))
        }
        Value::DateTime(value) | Value::Timestamp(value) => {
            let mut buffer = [0u8; 32];
            consume(crate::types::write_datetime_canonical_with_fsp(
                *value,
                column.and_then(|column| column.source.decimals),
                &mut buffer,
            ))
        }
        Value::Year(value) => {
            let mut buffer = [0u8; 32];
            consume(crate::types::write_year_canonical(*value, &mut buffer))
        }
        Value::Bytes(bytes) | Value::Geometry(bytes) => consume(bytes),
        Value::SmallString(bytes) => {
            if is_binary_charset(charset) {
                consume(bytes.as_slice())
            } else {
                let text = unsafe { std::str::from_utf8_unchecked(bytes.as_slice()) };
                let encoded = encode_text(charset, text)?;
                consume_owned_bytes(encoded, consume)
            }
        }
        Value::String(text) | Value::Decimal(text) => {
            if is_binary_charset(charset) {
                consume(text.as_bytes())
            } else {
                let encoded = encode_text(charset, text)?;
                consume_owned_bytes(encoded, consume)
            }
        }
        Value::Json(bytes) => {
            let text = crate::engine::json::to_text(bytes);
            let text_charge = ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::StorageScratch,
                text.capacity(),
            )?;
            if is_binary_charset(charset) {
                let result = consume(text.as_bytes());
                drop(text_charge);
                result
            } else {
                let encoded = encode_text(charset, &text)?;
                let result = consume_owned_bytes(encoded, consume);
                drop(text_charge);
                result
            }
        }
        Value::Vector(bytes) => {
            let text = crate::engine::vector::to_text(bytes);
            let text_charge = ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::StorageScratch,
                text.capacity(),
            )?;
            if is_binary_charset(charset) {
                let result = consume(text.as_bytes());
                drop(text_charge);
                result
            } else {
                let encoded = encode_text(charset, &text)?;
                let result = consume_owned_bytes(encoded, consume);
                drop(text_charge);
                result
            }
        }
        Value::Null => consume(&[]),
    }
}

fn consume_owned_bytes<R>(
    bytes: Vec<u8>,
    consume: impl FnOnce(&[u8]) -> Result<R, QueryError>,
) -> Result<R, QueryError> {
    let charge = ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::StorageScratch,
        bytes.capacity(),
    )?;
    let result = consume(&bytes);
    drop(charge);
    result
}

fn is_binary_charset(charset: &str) -> bool {
    charset.eq_ignore_ascii_case("binary")
        || charset.eq_ignore_ascii_case("utf8")
        || charset.eq_ignore_ascii_case("utf8mb3")
        || charset.eq_ignore_ascii_case("utf8mb4")
}

fn write_escaped(
    sink: &mut ExportSink,
    bytes: &[u8],
    options: &EncodedOptions,
    enclosure: Option<u8>,
) -> Result<(), QueryError> {
    let Some(escape) = options.escape else {
        return sink.write_all(bytes);
    };
    let field_first = options.field_terminated_by.first().copied();
    let line_first = options.line_terminated_by.first().copied();
    let mut start = 0usize;
    for (index, byte) in bytes.iter().copied().enumerate() {
        let escaped = if byte == 0 {
            Some(b'0')
        } else if byte == escape
            || enclosure == Some(byte)
            || (enclosure.is_none() && field_first == Some(byte))
            || line_first == Some(byte)
        {
            Some(byte)
        } else {
            None
        };
        if let Some(replacement) = escaped {
            sink.write_all(&bytes[start..index])?;
            sink.write_all(&[escape, replacement])?;
            start = index + 1;
        }
    }
    sink.write_all(&bytes[start..])
}

struct ExportSink {
    file: File,
    buffer: Vec<u8>,
    _writer_reservation: WriterReservation,
    sync_enabled: bool,
    sync_delay: Duration,
    path: PathBuf,
}

struct WriterReservation {
    charge: ControlledMemoryCharge,
    bytes: usize,
}

impl Drop for WriterReservation {
    fn drop(&mut self) {
        ACTIVE_WRITER_RESERVED_BYTES.fetch_sub(self.bytes, Ordering::Relaxed);
        let _ = &self.charge;
    }
}

impl ExportSink {
    fn write_all(&mut self, mut bytes: &[u8]) -> Result<(), QueryError> {
        while !bytes.is_empty() {
            if self.buffer.is_empty() && bytes.len() >= self.buffer.capacity() {
                self.file
                    .write_all(bytes)
                    .map_err(|err| self.write_error(err))?;
                if self.sync_enabled {
                    self.sync_and_delay()?;
                }
                return Ok(());
            }
            let available = self.buffer.capacity() - self.buffer.len();
            let take = available.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == self.buffer.capacity() {
                self.flush_buffer()?;
            }
        }
        Ok(())
    }

    fn flush_buffer(&mut self) -> Result<(), QueryError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.file
            .write_all(&self.buffer)
            .map_err(|err| self.write_error(err))?;
        self.buffer.clear();
        if self.sync_enabled {
            self.sync_and_delay()?;
        }
        Ok(())
    }

    fn sync_and_delay(&self) -> Result<(), QueryError> {
        self.file.sync_data().map_err(|err| self.write_error(err))?;
        if !self.sync_delay.is_zero() {
            std::thread::sleep(self.sync_delay);
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), QueryError> {
        self.flush_buffer()
    }

    fn write_error(&self, err: std::io::Error) -> QueryError {
        error(
            crate::error::catalog::ER_ERROR_ON_WRITE,
            format!(
                "Error writing file '{}' (OS errno {})",
                self.path.display(),
                err.raw_os_error().unwrap_or(0)
            ),
        )
    }
}

impl Drop for ExportSink {
    fn drop(&mut self) {
        // Runtime failures intentionally keep the partial file. Best-effort
        // flush makes rows already accepted by the sink visible even when a
        // later cursor row or DUMPFILE cardinality check fails.
        if !self.buffer.is_empty() {
            let _ = self.file.write_all(&self.buffer);
            self.buffer.clear();
            if self.sync_enabled {
                let _ = self.file.sync_data();
            }
        }
    }
}
