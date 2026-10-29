//! Generated MySQL catalog plus the hand-maintained MYDB extension range.

use super::{ErrorDomain, ErrorSpec};

mod mysql80_46_generated;

pub use mysql80_46_generated::*;

pub const MYSQL80_ERROR_MESSAGE_ELEMENTS_URL: &str =
    "https://dev.mysql.com/doc/refman/8.0/en/error-message-elements.html";
pub const MYSQL80_SERVER_ERROR_REFERENCE_URL: &str =
    "https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html";

pub const MYDB_ER_INVALID_VECTOR_VALUE: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50001,
    *b"22018",
    "MYDB_ER_INVALID_VECTOR_VALUE",
);
pub const MYDB_ER_VECTOR_DIMENSION_MISMATCH: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50002,
    *b"22023",
    "MYDB_ER_VECTOR_DIMENSION_MISMATCH",
);
pub const MYDB_ER_SEQUENCE_EXISTS: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50010,
    *b"42S01",
    "MYDB_ER_SEQUENCE_EXISTS",
);
pub const MYDB_ER_SEQUENCE_NOT_FOUND: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50011,
    *b"42S02",
    "MYDB_ER_SEQUENCE_NOT_FOUND",
);
pub const MYDB_ER_SEQUENCE_LIMIT_REACHED: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50012,
    *b"22003",
    "MYDB_ER_SEQUENCE_LIMIT_REACHED",
);
pub const MYDB_ER_SEQUENCE_VALUE_UNDEFINED: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50013,
    *b"55000",
    "MYDB_ER_SEQUENCE_VALUE_UNDEFINED",
);
pub const MYDB_ER_INVALID_SEQUENCE: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50014,
    *b"22023",
    "MYDB_ER_INVALID_SEQUENCE",
);
pub const MYDB_ER_SEQUENCE_CONTEXT_UNSUPPORTED: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50015,
    *b"0A000",
    "MYDB_ER_SEQUENCE_CONTEXT_UNSUPPORTED",
);
pub const MYDB_ER_UNDO_RETENTION_EXHAUSTED: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50020,
    *b"HY000",
    "MYDB_ER_UNDO_RETENTION_EXHAUSTED",
);
pub const MYDB_WARN_UNDO_CAPACITY: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50022,
    *b"01000",
    "MYDB_WARN_UNDO_CAPACITY",
);
pub const MYDB_ER_UNDO_TXN_LIMIT: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50021,
    *b"HY000",
    "MYDB_ER_UNDO_TXN_LIMIT",
);
pub const MYDB_ER_ROLE_CHANGE_BLOCKED: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50030,
    *b"55000",
    "MYDB_ER_ROLE_CHANGE_BLOCKED",
);
pub const MYDB_ER_REPLICATION_STATE: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50031,
    *b"HY000",
    "MYDB_ER_REPLICATION_STATE",
);
pub const MYDB_ER_REPLICATION_DIVERGED: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50032,
    *b"HY000",
    "MYDB_ER_REPLICATION_DIVERGED",
);
pub const MYDB_ER_REPLICATION_SECURITY: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50033,
    *b"28000",
    "MYDB_ER_REPLICATION_SECURITY",
);
pub const MYDB_ER_BASE_BACKUP_FAILED: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50034,
    *b"HY000",
    "MYDB_ER_BASE_BACKUP_FAILED",
);
pub const MYDB_ER_BACKUP_FAILED: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50035, *b"HY000", "MYDB_ER_BACKUP_FAILED");
pub const MYDB_ER_SYSTEM_TIME_TOO_OLD: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50040,
    *b"HY000",
    "MYDB_ER_SYSTEM_TIME_TOO_OLD",
);
pub const MYDB_ER_SYSTEM_TIME_SCHEMA_CHANGED: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50041,
    *b"HY000",
    "MYDB_ER_SYSTEM_TIME_SCHEMA_CHANGED",
);
pub const MYDB_ER_INVALID_SYSTEM_TIME: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50042,
    *b"22007",
    "MYDB_ER_INVALID_SYSTEM_TIME",
);
pub const MYDB_ER_SNAPSHOT_TOO_OLD: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50043,
    *b"HY000",
    "MYDB_ER_SNAPSHOT_TOO_OLD",
);

pub const CR_CONN_HOST_ERROR: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MysqlClient,
    2003,
    *b"HY000",
    "CR_CONN_HOST_ERROR",
);
pub const CR_SERVER_LOST: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MysqlClient, 2013, *b"HY000", "CR_SERVER_LOST");

pub const ER_AI_ACCESS_DENIED: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50050, *b"42000", "ER_AI_ACCESS_DENIED");
pub const ER_AI_BUDGET_EXCEEDED: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50051, *b"HY000", "ER_AI_BUDGET_EXCEEDED");
pub const ER_AI_CREDENTIAL_UNAVAILABLE: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50052,
    *b"HY000",
    "ER_AI_CREDENTIAL_UNAVAILABLE",
);
pub const ER_AI_DIMENSION_MISMATCH: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50053,
    *b"HY000",
    "ER_AI_DIMENSION_MISMATCH",
);
pub const ER_AI_DISABLED: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50054, *b"HY000", "ER_AI_DISABLED");
pub const ER_AI_EGRESS_DENIED: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50055, *b"42000", "ER_AI_EGRESS_DENIED");
pub const ER_AI_ENCRYPTION_KEY_INVALID: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50056,
    *b"HY000",
    "ER_AI_ENCRYPTION_KEY_INVALID",
);
pub const ER_AI_ENCRYPTION_KEY_MISSING: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50057,
    *b"HY000",
    "ER_AI_ENCRYPTION_KEY_MISSING",
);
pub const ER_AI_INCOMPLETE: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50058, *b"HY000", "ER_AI_INCOMPLETE");
pub const ER_AI_INVALID_ARGUMENT: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50059,
    *b"22023",
    "ER_AI_INVALID_ARGUMENT",
);
pub const ER_AI_INVALID_RESPONSE: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50060,
    *b"HY000",
    "ER_AI_INVALID_RESPONSE",
);
pub const ER_AI_MODEL_NOT_FOUND: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50061, *b"HY000", "ER_AI_MODEL_NOT_FOUND");
pub const ER_AI_OUTPUT_SCHEMA: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50062, *b"HY000", "ER_AI_OUTPUT_SCHEMA");
pub const ER_AI_QUEUE_FULL: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50063, *b"HY000", "ER_AI_QUEUE_FULL");
pub const ER_AI_REFUSED: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50064, *b"HY000", "ER_AI_REFUSED");
pub const ER_AI_SECRET_DECRYPT_FAILED: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50065,
    *b"HY000",
    "ER_AI_SECRET_DECRYPT_FAILED",
);
pub const ER_AI_TIMEOUT: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50066, *b"HY000", "ER_AI_TIMEOUT");
pub const ER_AI_UNSUPPORTED_CONTEXT: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50067,
    *b"0A000",
    "ER_AI_UNSUPPORTED_CONTEXT",
);
pub const ER_AI_UPSTREAM_: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50068, *b"HY000", "ER_AI_UPSTREAM_");
pub const ER_AI_UPSTREAM_AUTH: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50069, *b"HY000", "ER_AI_UPSTREAM_AUTH");
pub const ER_AI_UPSTREAM_ERROR: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50070, *b"HY000", "ER_AI_UPSTREAM_ERROR");
pub const ER_AI_WRONG_MODEL_KIND: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50071,
    *b"HY000",
    "ER_AI_WRONG_MODEL_KIND",
);

pub const ER_AI_CIRCUIT_OPEN: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50090, *b"HY000", "ER_AI_CIRCUIT_OPEN");

pub const ER_AI_KEY_ROTATION_INCOMPLETE: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50091,
    *b"HY000",
    "ER_AI_KEY_ROTATION_INCOMPLETE",
);

pub const MYDB_ERROR_SPECS: &[ErrorSpec] = &[
    MYDB_ER_INVALID_VECTOR_VALUE,
    MYDB_ER_VECTOR_DIMENSION_MISMATCH,
    MYDB_ER_SEQUENCE_EXISTS,
    MYDB_ER_SEQUENCE_NOT_FOUND,
    MYDB_ER_SEQUENCE_LIMIT_REACHED,
    MYDB_ER_SEQUENCE_VALUE_UNDEFINED,
    MYDB_ER_INVALID_SEQUENCE,
    MYDB_ER_SEQUENCE_CONTEXT_UNSUPPORTED,
    MYDB_ER_UNDO_RETENTION_EXHAUSTED,
    MYDB_ER_UNDO_TXN_LIMIT,
    MYDB_WARN_UNDO_CAPACITY,
    MYDB_ER_ROLE_CHANGE_BLOCKED,
    MYDB_ER_REPLICATION_STATE,
    MYDB_ER_REPLICATION_DIVERGED,
    MYDB_ER_REPLICATION_SECURITY,
    MYDB_ER_BASE_BACKUP_FAILED,
    MYDB_ER_BACKUP_FAILED,
    MYDB_ER_SYSTEM_TIME_TOO_OLD,
    MYDB_ER_SYSTEM_TIME_SCHEMA_CHANGED,
    MYDB_ER_INVALID_SYSTEM_TIME,
    MYDB_ER_SNAPSHOT_TOO_OLD,
    ER_AI_ACCESS_DENIED,
    ER_AI_BUDGET_EXCEEDED,
    ER_AI_CREDENTIAL_UNAVAILABLE,
    ER_AI_DIMENSION_MISMATCH,
    ER_AI_DISABLED,
    ER_AI_EGRESS_DENIED,
    ER_AI_ENCRYPTION_KEY_INVALID,
    ER_AI_ENCRYPTION_KEY_MISSING,
    ER_AI_INCOMPLETE,
    ER_AI_INVALID_ARGUMENT,
    ER_AI_INVALID_RESPONSE,
    ER_AI_MODEL_NOT_FOUND,
    ER_AI_OUTPUT_SCHEMA,
    ER_AI_QUEUE_FULL,
    ER_AI_REFUSED,
    ER_AI_SECRET_DECRYPT_FAILED,
    ER_AI_TIMEOUT,
    ER_AI_UNSUPPORTED_CONTEXT,
    ER_AI_UPSTREAM_,
    ER_AI_UPSTREAM_AUTH,
    ER_AI_UPSTREAM_ERROR,
    ER_AI_WRONG_MODEL_KIND,
    ER_AI_CIRCUIT_OPEN,
    ER_AI_KEY_ROTATION_INCOMPLETE,
];

pub fn lookup_by_code(code: u16) -> Option<&'static ErrorSpec> {
    MYSQL80_46_ERROR_SPECS
        .binary_search_by_key(&code, |spec| spec.code.get())
        .ok()
        .map(|idx| &MYSQL80_46_ERROR_SPECS[idx])
        .or_else(|| {
            MYDB_ERROR_SPECS
                .binary_search_by_key(&code, |spec| spec.code.get())
                .ok()
                .map(|idx| &MYDB_ERROR_SPECS[idx])
        })
}

#[cfg(test)]
pub fn lookup_by_symbol(symbol: &str) -> Option<&'static ErrorSpec> {
    MYSQL80_46_ERROR_SPECS
        .iter()
        .chain(MYDB_ERROR_SPECS)
        .find(|spec| spec.symbol == symbol)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn mysql_catalog_is_complete_sorted_and_unique() {
        assert_eq!(MYSQL80_46_ERROR_SPECS.len(), 1642);
        let mut codes = HashSet::new();
        let mut symbols = HashSet::new();
        let mut previous = 0;
        for spec in MYSQL80_46_ERROR_SPECS {
            assert_eq!(spec.domain, ErrorDomain::MysqlServer);
            assert!(spec.code.get() > previous);
            assert!(codes.insert(spec.code.get()));
            assert!(symbols.insert(spec.symbol));
            assert_eq!(spec.sql_state.as_bytes().len(), 5);
            previous = spec.code.get();
        }
    }

    #[test]
    fn mydb_range_is_reserved_and_disjoint() {
        let mut codes = HashSet::new();
        for spec in MYDB_ERROR_SPECS {
            assert!((50000..=50999).contains(&spec.code.get()));
            assert!(codes.insert(spec.code.get()));
            assert!(lookup_by_code(spec.code.get()).is_some());
        }
        assert!(MYSQL80_46_ERROR_SPECS
            .iter()
            .all(|spec| !(50000..=50999).contains(&spec.code.get())));
    }
}

pub const ER_HTTP_INVALID_ARGUMENT: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50100,
    *b"22023",
    "ER_HTTP_INVALID_ARGUMENT",
);

pub const ER_HTTP_ACCESS_DENIED: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50101, *b"42000", "ER_HTTP_ACCESS_DENIED");

pub const ER_HTTP_DISABLED: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50102, *b"HY000", "ER_HTTP_DISABLED");

pub const ER_HTTP_EGRESS_DENIED: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50103, *b"42000", "ER_HTTP_EGRESS_DENIED");

pub const ER_HTTP_BUDGET_EXCEEDED: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50104,
    *b"HY000",
    "ER_HTTP_BUDGET_EXCEEDED",
);

pub const ER_HTTP_QUEUE_FULL: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50105, *b"HY000", "ER_HTTP_QUEUE_FULL");

pub const ER_HTTP_DNS: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50106, *b"HY000", "ER_HTTP_DNS");

pub const ER_HTTP_CONNECT: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50107, *b"HY000", "ER_HTTP_CONNECT");

pub const ER_HTTP_TLS: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50108, *b"HY000", "ER_HTTP_TLS");

pub const ER_HTTP_TIMEOUT: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50109, *b"HY000", "ER_HTTP_TIMEOUT");

pub const ER_HTTP_INVALID_RESPONSE: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50110,
    *b"HY000",
    "ER_HTTP_INVALID_RESPONSE",
);

pub const ER_HTTP_TRANSPORT: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50111, *b"HY000", "ER_HTTP_TRANSPORT");

pub const ER_HTTP_UNSUPPORTED_CONTEXT: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50112,
    *b"HY000",
    "ER_HTTP_UNSUPPORTED_CONTEXT",
);

pub const ER_HTTP_REPLAY_FORBIDDEN: ErrorSpec = ErrorSpec::new(
    ErrorDomain::MyDb,
    50113,
    *b"HY000",
    "ER_HTTP_REPLAY_FORBIDDEN",
);

pub const ER_HTTP_RATE_LIMITED: ErrorSpec =
    ErrorSpec::new(ErrorDomain::MyDb, 50114, *b"HY000", "ER_HTTP_RATE_LIMITED");
