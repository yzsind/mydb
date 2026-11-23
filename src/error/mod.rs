//! Strongly typed SQL conditions and the MySQL 8.0.46 error catalog.
//!
//! Client-visible conditions must be created from an [`ErrorSpec`].  The
//! only exception is SQL `SIGNAL`/`RESIGNAL`, whose user-supplied errno and
//! SQLSTATE are validated by [`QueryError::user_defined`].

use std::fmt;
use std::num::NonZeroU16;
use std::ops::Deref;

pub mod catalog;
pub mod conditions;
pub mod messages;

/// A non-zero MySQL protocol error number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct ErrorNumber(NonZeroU16);

impl ErrorNumber {
    pub const fn new(value: u16) -> Option<Self> {
        match NonZeroU16::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn new_const(value: u16) -> Self {
        match Self::new(value) {
            Some(value) => value,
            None => panic!("SQL condition errno must be non-zero"),
        }
    }

    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

impl fmt::Display for ErrorNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(f)
    }
}

impl PartialEq<u16> for ErrorNumber {
    fn eq(&self, other: &u16) -> bool {
        self.get() == *other
    }
}

impl PartialEq<ErrorNumber> for u16 {
    fn eq(&self, other: &ErrorNumber) -> bool {
        *self == other.get()
    }
}

/// A validated five-byte SQLSTATE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SqlState([u8; 5]);

impl SqlState {
    pub const HY000: Self = Self::new_const(*b"HY000");

    pub const fn new_const(bytes: [u8; 5]) -> Self {
        let mut idx = 0;
        while idx < bytes.len() {
            let byte = bytes[idx];
            if !((byte >= b'0' && byte <= b'9') || (byte >= b'A' && byte <= b'Z')) {
                panic!("SQLSTATE must contain exactly five uppercase ASCII alphanumerics");
            }
            idx += 1;
        }
        Self(bytes)
    }

    pub fn parse(value: &str) -> Result<Self, InvalidSqlState> {
        let bytes = value.as_bytes();
        if bytes.len() != 5
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_digit() || byte.is_ascii_uppercase())
        {
            return Err(InvalidSqlState);
        }
        let mut out = [0_u8; 5];
        out.copy_from_slice(bytes);
        Ok(Self(out))
    }

    pub const fn as_bytes(&self) -> &[u8; 5] {
        &self.0
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("validated SQLSTATE is ASCII")
    }

    pub const fn is_success(self) -> bool {
        self.0[0] == b'0' && self.0[1] == b'0'
    }
}

impl fmt::Display for SqlState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for SqlState {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for SqlState {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl PartialEq<&str> for SqlState {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<SqlState> for &str {
    fn eq(&self, other: &SqlState) -> bool {
        *self == other.as_str()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSqlState;

impl fmt::Display for InvalidSqlState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SQLSTATE must contain exactly five uppercase ASCII alphanumerics")
    }
}

impl std::error::Error for InvalidSqlState {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorDomain {
    MysqlServer,
    MysqlClient,
    MyDb,
}

/// Immutable metadata for a known SQL condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorSpec {
    code: ErrorNumber,
    sql_state: SqlState,
    symbol: &'static str,
    domain: ErrorDomain,
}

impl ErrorSpec {
    pub(crate) const fn new(
        domain: ErrorDomain,
        code: u16,
        sql_state: [u8; 5],
        symbol: &'static str,
    ) -> Self {
        if symbol.is_empty() {
            panic!("SQL condition symbol must not be empty");
        }
        Self {
            code: ErrorNumber::new_const(code),
            sql_state: SqlState::new_const(sql_state),
            symbol,
            domain,
        }
    }

    pub fn error(self, message: impl Into<String>) -> QueryError {
        QueryError::known(self, message)
    }

    pub const fn code(self) -> ErrorNumber {
        self.code
    }

    pub const fn sql_state(self) -> SqlState {
        self.sql_state
    }

    pub const fn symbol(self) -> &'static str {
        self.symbol
    }

    pub const fn domain(self) -> ErrorDomain {
        self.domain
    }

    pub fn diagnostic(
        self,
        message: impl Into<String>,
        id: &'static str,
        detail: impl Into<String>,
    ) -> QueryError {
        QueryError::known(self, message).with_diagnostic(id, detail)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConditionLevel {
    Error,
    Warning,
    Note,
}

impl ConditionLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Error => "Error",
            Self::Warning => "Warning",
            Self::Note => "Note",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalDiagnostic {
    pub id: &'static str,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum InternalErrorKind {
    LockWouldBlock,
    LockWaitPending,
    HashAggregateNeedsSpill,
    VectorHashJoinFallback,
    ReplicaPoisonFrame,
    ReplicaApplyFailure,
    ReplicaSnapshotBlocked,
    ReplicaHandshakeRejected,
    ShutdownRecoveryRequired,
}

/// A span in the original SQL supplied by the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub(crate) struct SqlPosition {
    pub line: usize,
    pub column: usize,
    pub byte_start: usize,
    pub byte_end: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ConditionDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<SqlPosition>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub expected: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clause: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_truncated: Option<bool>,
}

impl ConditionDetails {
    fn information_item(&self, name: &str) -> Option<&str> {
        match name {
            "SCHEMA_NAME" => self.schema.as_deref(),
            "TABLE_NAME" => self.table.as_deref(),
            "COLUMN_NAME" => self.column.as_deref(),
            "CONSTRAINT_NAME" => self.constraint.as_deref(),
            "CONSTRAINT_SCHEMA" if self.constraint.is_some() => self.schema.as_deref(),
            _ => None,
        }
    }
}

/// Optional SQL condition data. Keeping it behind one pointer avoids
/// growing the error arm of results on storage and execution hot paths.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ConditionMetadata {
    details: Option<ConditionDetails>,
    information: std::collections::BTreeMap<&'static str, String>,
}

impl ConditionMetadata {
    pub(crate) fn details(&self) -> Option<&ConditionDetails> {
        self.details.as_ref()
    }
}

/// Source ordinals for INSERT/REPLACE. A fragment may carry the starting
/// ordinal of a contiguous range, or no ordinal when the order is unknown.
#[derive(Clone, Copy, Default)]
pub(crate) struct InputRows {
    first: Option<u64>,
    pub direct_values: bool,
}

thread_local! {
    static INPUT_ROWS: std::cell::Cell<InputRows> = const {
        std::cell::Cell::new(InputRows { first: None, direct_values: false })
    };
}

pub(crate) struct InputRowsGuard(InputRows, std::marker::PhantomData<std::rc::Rc<()>>);

impl InputRows {
    pub(crate) fn new(first: u64, direct_values: bool) -> Self {
        Self {
            first: Some(first),
            direct_values,
        }
    }

    pub(crate) fn current() -> Self {
        INPUT_ROWS.get()
    }

    pub(crate) fn enter(self) -> InputRowsGuard {
        InputRowsGuard(INPUT_ROWS.replace(self), std::marker::PhantomData)
    }

    pub(crate) fn row(self, offset: usize) -> Option<u64> {
        self.first.map(|first| first + offset as u64)
    }

    pub(crate) fn offset(self, offset: usize) -> Self {
        Self {
            first: self.row(offset),
            ..self
        }
    }

    pub(crate) fn annotate(self, offset: usize, error: QueryError) -> QueryError {
        match self.row(offset) {
            Some(row) => error.with_input_row(row),
            None => error,
        }
    }
}

impl Drop for InputRowsGuard {
    fn drop(&mut self) {
        INPUT_ROWS.set(self.0);
    }
}

thread_local! {
    static INPUT_PREVIEW_COLUMNS: std::cell::RefCell<Option<Vec<usize>>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) struct InputPreviewColumns(Option<Vec<usize>>, std::marker::PhantomData<std::rc::Rc<()>>);

impl InputPreviewColumns {
    /// Take ownership of the INSERT projection after the row has been
    /// expanded. The existing column indexes stay available on failure.
    pub(crate) fn enter(columns: Option<Vec<usize>>) -> Self {
        Self(INPUT_PREVIEW_COLUMNS.replace(columns), std::marker::PhantomData)
    }

    pub(crate) fn includes(index: usize) -> bool {
        INPUT_PREVIEW_COLUMNS.with(|columns| {
            columns.borrow().as_ref().is_none_or(|columns| columns.contains(&index))
        })
    }
}

impl Drop for InputPreviewColumns {
    fn drop(&mut self) {
        INPUT_PREVIEW_COLUMNS.replace(self.0.take());
    }
}

const MAX_VALUE_PREVIEW_BYTES: usize = 128;

fn text_preview(value: &str) -> (String, bool) {
    let mut preview = String::new();
    for ch in value.chars() {
        if ch.is_control() {
            let escaped = ch.escape_default();
            if preview.len() + escaped.len() > MAX_VALUE_PREVIEW_BYTES {
                return (preview, true);
            }
            preview.extend(escaped);
        } else {
            if preview.len() + ch.len_utf8() > MAX_VALUE_PREVIEW_BYTES {
                return (preview, true);
            }
            preview.push(ch);
        }
    }
    (preview, false)
}

fn binary_preview(value: &[u8]) -> (String, bool) {
    use std::fmt::Write;
    let limit = (MAX_VALUE_PREVIEW_BYTES - 2) / 2;
    let mut preview = String::from("0x");
    for byte in value.iter().take(limit) {
        write!(preview, "{byte:02X}").expect("writing to String");
    }
    (preview, value.len() > limit)
}

/// Error returned through the query engine and MySQL protocol.
///
/// Fields are private: callers can only inspect a condition through the
/// read-only accessors below. A source audit test also forbids direct
/// construction outside this module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryError {
    code: u16,
    sql_state: SqlState,
    message: String,
    spec: Option<ErrorSpec>,
    diagnostic: Option<InternalDiagnostic>,
    followup_errors: Option<std::sync::Arc<Vec<QueryError>>>,
    internal_kind: Option<InternalErrorKind>,
    metadata: Option<std::sync::Arc<ConditionMetadata>>,
}

impl QueryError {
    pub fn known(spec: ErrorSpec, message: impl Into<String>) -> Self {
        Self {
            code: spec.code.get(),
            sql_state: spec.sql_state,
            message: message.into(),
            spec: Some(spec),
            diagnostic: None,
            followup_errors: None,
            internal_kind: None,
            metadata: None,
        }
    }

    /// Construct a dynamic condition for SQL `SIGNAL`/`RESIGNAL`.
    pub fn user_defined(
        code: u16,
        sql_state: &str,
        message: impl Into<String>,
    ) -> Result<Self, InvalidUserCondition> {
        let code = ErrorNumber::new(code).ok_or(InvalidUserCondition::ZeroErrno)?;
        let sql_state =
            SqlState::parse(sql_state).map_err(|_| InvalidUserCondition::InvalidSqlState)?;
        if sql_state.is_success() {
            return Err(InvalidUserCondition::SuccessSqlState);
        }
        Ok(Self {
            code: code.get(),
            sql_state,
            message: message.into(),
            spec: None,
            diagnostic: None,
            followup_errors: None,
            internal_kind: None,
            metadata: None,
        })
    }

    pub(crate) fn with_condition_information(
        mut self,
        information: std::collections::BTreeMap<&'static str, String>,
    ) -> Self {
        if information.is_empty() && self.details().is_none() {
            self.metadata = None;
        } else {
            std::sync::Arc::make_mut(self.metadata.get_or_insert_with(Default::default))
                .information = information;
        }
        self
    }

    pub(crate) fn condition_information(&self) -> std::collections::BTreeMap<&'static str, String> {
        self.metadata
            .as_ref()
            .map(|metadata| metadata.information.clone())
            .unwrap_or_default()
    }

    pub(crate) fn details(&self) -> Option<&ConditionDetails> {
        self.metadata
            .as_deref()
            .and_then(ConditionMetadata::details)
    }

    pub(crate) fn shared_metadata(&self) -> Option<std::sync::Arc<ConditionMetadata>> {
        self.metadata.clone()
    }

    pub(crate) fn with_shared_metadata(
        mut self,
        metadata: Option<std::sync::Arc<ConditionMetadata>>,
    ) -> Self {
        self.metadata = metadata;
        self
    }

    pub(crate) fn with_details(mut self, update: impl FnOnce(&mut ConditionDetails)) -> Self {
        let metadata = std::sync::Arc::make_mut(self.metadata.get_or_insert_with(Default::default));
        update(metadata.details.get_or_insert_with(Default::default));
        self
    }

    pub(crate) fn with_table(self, schema: &str, table: &str) -> Self {
        self.with_details(|details| {
            if !schema.is_empty() {
                details.schema.get_or_insert_with(|| schema.to_owned());
            }
            details.table.get_or_insert_with(|| table.to_owned());
        })
    }

    pub(crate) fn describe_target(mut self) -> Self {
        use std::fmt::Write;
        if let Some(details) = self
            .metadata
            .as_deref()
            .and_then(ConditionMetadata::details)
        {
            if let Some(table) = &details.table {
                self.message.push_str(" (table '");
                if let Some(schema) = &details.schema {
                    write!(self.message, "{schema}.").expect("writing to String");
                }
                write!(self.message, "{table}')").expect("writing to String");
            }
        }
        self
    }

    pub(crate) fn describe_value(mut self) -> Self {
        use std::fmt::Write;
        if let Some(details) = self
            .metadata
            .as_deref()
            .and_then(ConditionMetadata::details)
        {
            if let Some(value) = &details.value_preview {
                let suffix = if details.value_truncated == Some(true) {
                    "…"
                } else {
                    ""
                };
                write!(self.message, " (value {value:?}{suffix})").expect("writing to String");
            }
        }
        self
    }

    pub(crate) fn with_column(self, column: &str) -> Self {
        self.with_details(|details| {
            details.column.get_or_insert_with(|| column.to_owned());
        })
    }

    pub(crate) fn with_input_row(mut self, row: u64) -> Self {
        if self
            .details()
            .is_some_and(|details| details.row_number.is_some())
        {
            return self;
        }
        if let Some(base) = self.message.strip_suffix(" at row 1") {
            self.message.truncate(base.len());
        }
        use std::fmt::Write;
        write!(self.message, " at row {row}").expect("writing to String");
        self.with_details(|details| details.row_number = Some(row))
    }

    pub(crate) fn with_value_preview(self, value: &crate::types::Value) -> Self {
        use crate::types::Value;
        let preview = match value {
            Value::String(text) | Value::Decimal(text) => text_preview(text),
            Value::SmallString(_) => {
                text_preview(crate::types::value_str(value).unwrap_or_default())
            }
            Value::Bytes(bytes) | Value::Json(bytes) | Value::Geometry(bytes) => {
                binary_preview(bytes)
            }
            Value::Vector(vector) => binary_preview(&vector.payload),
            value => text_preview(&value.to_display_string()),
        };
        self.with_preview(preview)
    }

    pub(crate) fn with_text_preview(self, value: &str) -> Self {
        self.with_preview(text_preview(value))
    }

    pub(crate) fn with_field_preview(self, value: &[u8], binary: bool) -> Self {
        let preview = match std::str::from_utf8(value) {
            Ok(text) if !binary => text_preview(text),
            _ => binary_preview(value),
        };
        self.with_preview(preview)
    }

    fn with_preview(self, (preview, truncated): (String, bool)) -> Self {
        self.with_details(|details| {
            details.value_preview = Some(preview);
            details.value_truncated = Some(truncated);
        })
    }

    /// Translate a syntax position in a statement suffix to the full request.
    pub(crate) fn rebase_sql_position(mut self, source: &str, byte_offset: usize) -> Self {
        let Some(position) = self.details().and_then(|details| details.position) else {
            return self;
        };
        let byte_start = byte_offset + position.byte_start;
        let byte_end = byte_offset + position.byte_end;
        let Some(prefix) = source.get(..byte_start) else {
            return self;
        };
        if !source.is_char_boundary(byte_end) {
            return self;
        }
        let line = prefix.bytes().filter(|&byte| byte == b'\n').count() + 1;
        let column = prefix
            .rsplit('\n')
            .next()
            .unwrap_or_default()
            .chars()
            .count()
            + 1;
        if byte_offset == 0 && line == position.line && column == position.column {
            return self;
        }
        let location = format!(" at line {}, column {}", position.line, position.column);
        if let Some(index) = self.message.rfind(&location) {
            self.message.replace_range(
                index..index + location.len(),
                &format!(" at line {line}, column {column}"),
            );
        }
        self.with_details(|details| {
            details.position = Some(SqlPosition {
                line,
                column,
                byte_start,
                byte_end,
            });
        })
    }

    /// Additional server conditions reported after the primary failure.
    pub(crate) fn with_followup_error(mut self, error: QueryError) -> Self {
        std::sync::Arc::make_mut(self.followup_errors.get_or_insert_with(Default::default))
            .push(error);
        self
    }

    pub(crate) fn followup_errors(&self) -> &[QueryError] {
        self.followup_errors.as_deref().map_or(&[], Vec::as_slice)
    }

    pub fn with_diagnostic(mut self, id: &'static str, detail: impl Into<String>) -> Self {
        self.diagnostic = Some(InternalDiagnostic {
            id,
            detail: detail.into(),
        });
        self
    }

    pub(crate) fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }

    pub(crate) fn map_message(mut self, map: impl FnOnce(&str) -> String) -> Self {
        self.message = map(&self.message);
        self
    }

    pub(crate) fn internal_control(
        kind: InternalErrorKind,
        fallback: ErrorSpec,
        message: impl Into<String>,
    ) -> Self {
        let mut error = Self::known(fallback, message);
        error.internal_kind = Some(kind);
        error
    }

    pub(crate) const fn is_internal(&self, kind: InternalErrorKind) -> bool {
        matches!(self.internal_kind, Some(current) if current as u8 == kind as u8)
    }

    pub const fn code(&self) -> u16 {
        self.code
    }

    pub const fn sql_state(&self) -> SqlState {
        self.sql_state
    }

    pub fn symbol(&self) -> Option<&'static str> {
        self.spec.map(|spec| spec.symbol).or_else(|| {
            catalog::lookup_by_code(self.code())
                .filter(|spec| spec.sql_state == self.sql_state)
                .map(|spec| spec.symbol)
        })
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub const fn spec(&self) -> Option<ErrorSpec> {
        self.spec
    }

    pub fn is(&self, spec: ErrorSpec) -> bool {
        self.code == spec.code.get() && self.sql_state == spec.sql_state
    }

    pub fn is_any(&self, specs: &[ErrorSpec]) -> bool {
        specs.iter().copied().any(|spec| self.is(spec))
    }

    pub fn diagnostic_id(&self) -> Option<&'static str> {
        self.diagnostic.as_ref().map(|diagnostic| diagnostic.id)
    }

    pub fn diagnostic_detail(&self) -> Option<&str> {
        self.diagnostic
            .as_ref()
            .map(|diagnostic| diagnostic.detail.as_str())
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}/{}", self.message, self.code, self.sql_state)?;
        if let Some(symbol) = self.symbol() {
            write!(f, ", {symbol}")?;
        }
        f.write_str(")")
    }
}

impl std::error::Error for QueryError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidUserCondition {
    ZeroErrno,
    InvalidSqlState,
    SuccessSqlState,
}

impl fmt::Display for InvalidUserCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroErrno => f.write_str("MYSQL_ERRNO must be in 1..=65535"),
            Self::InvalidSqlState => InvalidSqlState.fmt(f),
            Self::SuccessSqlState => {
                f.write_str("SIGNAL/RESIGNAL SQLSTATE must not use success class 00")
            }
        }
    }
}

impl std::error::Error for InvalidUserCondition {}

/// A statement condition retained for `SHOW WARNINGS` and diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCondition {
    level: ConditionLevel,
    code: ErrorNumber,
    sql_state: SqlState,
    symbol: Option<&'static str>,
    message: std::borrow::Cow<'static, str>,
    metadata: Option<std::sync::Arc<ConditionMetadata>>,
}

impl SqlCondition {
    pub fn known(level: ConditionLevel, spec: ErrorSpec, message: impl Into<String>) -> Self {
        Self {
            level,
            code: spec.code,
            sql_state: spec.sql_state,
            symbol: Some(spec.symbol),
            message: std::borrow::Cow::Owned(message.into()),
            metadata: None,
        }
    }

    /// Retain a literal diagnostic without allocating a copy for every row.
    /// Dynamic messages continue to use `known` and own their contents.
    pub fn known_static(level: ConditionLevel, spec: ErrorSpec, message: &'static str) -> Self {
        Self {
            level,
            code: spec.code,
            sql_state: spec.sql_state,
            symbol: Some(spec.symbol),
            message: std::borrow::Cow::Borrowed(message),
            metadata: None,
        }
    }

    pub fn from_error(level: ConditionLevel, error: &QueryError) -> Self {
        Self {
            level,
            code: ErrorNumber::new_const(error.code()),
            sql_state: error.sql_state(),
            symbol: error.symbol(),
            message: std::borrow::Cow::Owned(
                error
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.information.get("MESSAGE_TEXT"))
                    .cloned()
                    .unwrap_or_else(|| error.message().to_string()),
            ),
            metadata: error.metadata.clone(),
        }
    }

    pub(crate) fn information_item(&self, name: &str) -> Option<&str> {
        let metadata = self.metadata.as_deref()?;
        metadata
            .information
            .get(name)
            .map(String::as_str)
            .or_else(|| metadata.details()?.information_item(name))
    }

    pub(crate) fn details(&self) -> Option<&ConditionDetails> {
        self.metadata
            .as_deref()
            .and_then(ConditionMetadata::details)
    }

    pub(crate) fn shared_metadata(&self) -> Option<std::sync::Arc<ConditionMetadata>> {
        self.metadata.clone()
    }

    pub const fn level(&self) -> ConditionLevel {
        self.level
    }

    pub const fn code(&self) -> u16 {
        self.code.get()
    }

    pub const fn sql_state(&self) -> SqlState {
        self.sql_state
    }

    pub const fn symbol(&self) -> Option<&'static str> {
        self.symbol
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is(&self, spec: ErrorSpec) -> bool {
        self.code == spec.code && self.sql_state == spec.sql_state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_conditions_preserve_owned_diagnostic_semantics() {
        let spec = catalog::ER_WARN_TOO_MANY_RECORDS;
        let borrowed = SqlCondition::known_static(ConditionLevel::Warning, spec, "extra field");
        let owned = SqlCondition::known(ConditionLevel::Warning, spec, "extra field");
        assert_eq!(borrowed, owned);
        assert_eq!(borrowed.clone(), owned.clone());
        assert!(matches!(borrowed.message, std::borrow::Cow::Borrowed(_)));
        assert!(matches!(owned.message, std::borrow::Cow::Owned(_)));
        assert_eq!(
            borrowed.message().as_ptr(),
            borrowed.clone().message().as_ptr()
        );
        let dynamic = String::from("dynamic diagnostic");
        let retained = SqlCondition::known(ConditionLevel::Warning, spec, dynamic.as_str());
        drop(dynamic);
        assert_eq!(retained.message(), "dynamic diagnostic");
    }

    #[test]
    fn shared_condition_metadata_preserves_details_when_information_changes() {
        let original = catalog::ER_BAD_NULL_ERROR
            .error("cannot be null")
            .with_table("test", "orders")
            .with_column("customer_id");
        let resignaled = QueryError::user_defined(1644, "45000", "raised by handler")
            .unwrap()
            .with_shared_metadata(original.shared_metadata())
            .with_condition_information(std::collections::BTreeMap::from([(
                "TABLE_NAME",
                "reported_orders".to_owned(),
            )]));
        let condition = SqlCondition::from_error(ConditionLevel::Error, &resignaled);

        assert_eq!(condition.details(), original.details());
        assert_eq!(
            condition.information_item("TABLE_NAME"),
            Some("reported_orders")
        );
        assert_eq!(
            condition.information_item("COLUMN_NAME"),
            Some("customer_id")
        );
        assert!(original.condition_information().is_empty());

        let signal = QueryError::user_defined(1644, "45000", "signal")
            .unwrap()
            .with_condition_information(resignaled.condition_information());
        let signal = SqlCondition::from_error(ConditionLevel::Error, &signal);
        assert!(signal.details().is_none());
        assert_eq!(signal.information_item("TABLE_NAME"), Some("reported_orders"));
    }

    #[test]
    fn sql_state_validation_is_strict() {
        assert_eq!(SqlState::parse("42S02").unwrap().as_str(), "42S02");
        assert!(SqlState::parse("hy000").is_err());
        assert!(SqlState::parse("HY00").is_err());
        assert!(SqlState::parse("HY00!").is_err());
    }

    #[test]
    fn signal_rejects_zero_and_success_state() {
        assert_eq!(
            QueryError::user_defined(0, "45000", "x").unwrap_err(),
            InvalidUserCondition::ZeroErrno
        );
        assert_eq!(
            QueryError::user_defined(1000, "00000", "x").unwrap_err(),
            InvalidUserCondition::SuccessSqlState
        );
    }
}
