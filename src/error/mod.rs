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
    ReplicaHandshakeRejected,
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
    internal_kind: Option<InternalErrorKind>,
    condition_information: Option<std::sync::Arc<std::collections::BTreeMap<&'static str, String>>>,
}

impl QueryError {
    pub fn known(spec: ErrorSpec, message: impl Into<String>) -> Self {
        Self {
            code: spec.code.get(),
            sql_state: spec.sql_state,
            message: message.into(),
            spec: Some(spec),
            diagnostic: None,
            internal_kind: None,
            condition_information: None,
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
            internal_kind: None,
            condition_information: None,
        })
    }

    pub(crate) fn with_condition_information(
        mut self,
        information: std::collections::BTreeMap<&'static str, String>,
    ) -> Self {
        self.condition_information =
            (!information.is_empty()).then(|| std::sync::Arc::new(information));
        self
    }

    pub(crate) fn condition_information(&self) -> std::collections::BTreeMap<&'static str, String> {
        self.condition_information
            .as_deref()
            .cloned()
            .unwrap_or_default()
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

    #[cfg(test)]
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
    condition_information: Option<std::sync::Arc<std::collections::BTreeMap<&'static str, String>>>,
}

impl SqlCondition {
    pub fn known(level: ConditionLevel, spec: ErrorSpec, message: impl Into<String>) -> Self {
        Self {
            level,
            code: spec.code,
            sql_state: spec.sql_state,
            symbol: Some(spec.symbol),
            message: std::borrow::Cow::Owned(message.into()),
            condition_information: None,
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
            condition_information: None,
        }
    }

    pub fn from_error(level: ConditionLevel, error: &QueryError) -> Self {
        Self {
            level,
            code: ErrorNumber::new_const(error.code()),
            sql_state: error.sql_state(),
            symbol: error.symbol(),
            message: std::borrow::Cow::Owned(error.message().to_string()),
            condition_information: error.condition_information.clone(),
        }
    }

    pub(crate) fn condition_information(&self) -> std::collections::BTreeMap<&'static str, String> {
        self.condition_information
            .as_deref()
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn information_item(&self, name: &str) -> Option<&str> {
        self.condition_information
            .as_ref()?
            .get(name)
            .map(String::as_str)
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
