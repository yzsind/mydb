//! Bounded HTTP SQL effects, independent of AI model execution.
mod config;
mod json;
mod request;
mod runtime;
mod scope;
mod sql;
pub(crate) use runtime::{admit_coordinator, admit_session, spawn_coordinator};
pub(crate) use sql::{bound_sql, may_call, sensitive};
#[cfg(test)]
mod tests;
use crate::types::QueryError;
pub(crate) use config::{enabled, reload, set_enabled};
pub(crate) use runtime::{call, shutdown_database, status};
pub(crate) use scope::{
    activate, capture, enter, enter_context, fresh_statement, sent, wrap_response, Context,
};
pub(crate) fn error(code: &str, message: &str) -> QueryError {
    use crate::error::catalog::*;
    let spec = match code {
        "ER_HTTP_INVALID_ARGUMENT" => ER_HTTP_INVALID_ARGUMENT,
        "ER_HTTP_ACCESS_DENIED" => ER_HTTP_ACCESS_DENIED,
        "ER_HTTP_DISABLED" => ER_HTTP_DISABLED,
        "ER_HTTP_EGRESS_DENIED" => ER_HTTP_EGRESS_DENIED,
        "ER_HTTP_BUDGET_EXCEEDED" => ER_HTTP_BUDGET_EXCEEDED,
        "ER_HTTP_QUEUE_FULL" => ER_HTTP_QUEUE_FULL,
        "ER_HTTP_DNS" => ER_HTTP_DNS,
        "ER_HTTP_CONNECT" => ER_HTTP_CONNECT,
        "ER_HTTP_TLS" => ER_HTTP_TLS,
        "ER_HTTP_TIMEOUT" => ER_HTTP_TIMEOUT,
        "ER_HTTP_INVALID_RESPONSE" => ER_HTTP_INVALID_RESPONSE,
        "ER_HTTP_TRANSPORT" => ER_HTTP_TRANSPORT,
        "ER_HTTP_UNSUPPORTED_CONTEXT" => ER_HTTP_UNSUPPORTED_CONTEXT,
        "ER_HTTP_REPLAY_FORBIDDEN" => ER_HTTP_REPLAY_FORBIDDEN,
        "ER_HTTP_RATE_LIMITED" => ER_HTTP_RATE_LIMITED,
        _ => ER_HTTP_TRANSPORT,
    };
    spec.error(format!("{code}: {message}"))
}
pub(crate) fn is_function(name: &str) -> bool {
    if !name
        .as_bytes()
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"HTTP_"))
    {
        return false;
    }
    [
        "HTTP_REQUEST",
        "HTTP_GET",
        "HTTP_HEAD",
        "HTTP_OPTIONS",
        "HTTP_DELETE",
        "HTTP_POST",
        "HTTP_PUT",
        "HTTP_PATCH",
    ]
    .iter()
    .any(|n| name.eq_ignore_ascii_case(n))
}
/// Conservative allocation-free scheduling hint; authoritative checks use ASTs.
pub(crate) fn mentions(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    for i in memchr::memchr2_iter(b'h', b'H', bytes) {
        if i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || matches!(bytes[i - 1], b'_' | b'$')) {
            continue;
        }
        for name in [
            b"HTTP_REQUEST".as_slice(),
            b"HTTP_GET",
            b"HTTP_HEAD",
            b"HTTP_OPTIONS",
            b"HTTP_DELETE",
            b"HTTP_POST",
            b"HTTP_PUT",
            b"HTTP_PATCH",
        ] {
            if !bytes
                .get(i..i + name.len())
                .is_some_and(|w| w.eq_ignore_ascii_case(name))
            {
                continue;
            }
            let mut end = i + name.len();
            if i > 0 && matches!(bytes[i - 1], b'`' | b'"') && bytes.get(end) == Some(&bytes[i - 1])
            {
                end += 1;
            }
            while bytes.get(end).is_some_and(u8::is_ascii_whitespace) {
                end += 1;
            }
            if bytes.get(end) == Some(&b'(')
                || bytes.get(end..end + 2) == Some(b"/*")
                || bytes.get(end..end + 2) == Some(b"--")
                || bytes.get(end) == Some(&b'#')
            {
                return true;
            }
        }
    }
    false
}
pub(crate) fn statement_has_http(stmt: &crate::parser::Statement) -> bool {
    crate::parser::collect_statement_function_names(stmt)
        .iter()
        .any(|n| is_function(n))
}
pub(crate) fn authorize(
    db: &crate::storage::Database,
    ctx: &crate::engine::auth::AuthorizationContext,
    privilege: &str,
) -> Result<(), QueryError> {
    use crate::engine::auth::{Privilege, PrivilegeScope, RequiredPrivilege};
    if !db.auth_catalog().account_has_privilege(
        ctx,
        &RequiredPrivilege {
            privilege: Privilege::Dynamic(privilege.into()),
            scope: PrivilegeScope::Global,
        },
        false,
    ) {
        return Err(error("ER_HTTP_ACCESS_DENIED", "HTTP privilege required"));
    }
    Ok(())
}

pub(crate) fn redaction_marker(sql: &str, is_error: bool) -> &'static str {
    match (crate::engine::ai::sql::sql_may_have_ai(sql), is_error) {
        (true, true) => "[redacted AI statement error]",
        (true, false) => "[redacted AI statement]",
        (false, true) => "[redacted HTTP statement error]",
        (false, false) => "[redacted HTTP statement]",
    }
}
