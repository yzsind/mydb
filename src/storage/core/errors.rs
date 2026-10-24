fn io_err(err: std::io::Error) -> QueryError {
    error(crate::error::catalog::ER_GET_ERRNO, format!("storage I/O error: {err}"))
}

pub(crate) fn json_err(err: serde_json::Error) -> QueryError {
    error(crate::error::catalog::ER_GET_ERRNO, format!("storage metadata error: {err}"))
}
pub fn error(
    spec: crate::error::ErrorSpec,
    message: impl Into<String>,
) -> QueryError {
    spec.error(message)
}
