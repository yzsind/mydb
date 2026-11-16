use crate::types::QueryError;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
pub type Result<T> = std::result::Result<T, Failure>;
#[derive(Debug)]
pub struct Failure {
    pub status: StatusCode,
    pub code: String,
    pub message: String,
    pub message_truncated: bool,
    pub mysql_errno: Option<u16>,
    pub sqlstate: Option<String>,
    pub request_id: Option<String>,
    pub execution_outcome: &'static str,
}
impl Failure {
    pub fn new(status: StatusCode, code: &str, message: impl Into<String>) -> Self {
        let mut message = message.into();
        let truncated = message.len() > 2048;
        if truncated {
            let mut end = 2048;
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message.truncate(end);
        }
        Self {
            status,
            code: code.into(),
            message,
            message_truncated: truncated,
            mysql_errno: None,
            sqlstate: None,
            request_id: None,
            execution_outcome: "not_started",
        }
    }
    pub fn with_request_id(mut self, id: String) -> Self {
        self.request_id = Some(id);
        self
    }
    pub fn with_outcome(mut self, outcome: &'static str) -> Self {
        self.execution_outcome = outcome;
        self
    }
    pub fn bad(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "INVALID_REQUEST", message)
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", message)
    }
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "CAPACITY_EXCEEDED",
            message,
        )
    }
    pub fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "NOT_FOUND", "Resource not found")
    }
    pub fn value(&self) -> Value {
        json!({"code":self.code,"message":self.message,"message_truncated":self.message_truncated,"mysql_errno":self.mysql_errno,"sqlstate":self.sqlstate})
    }
}
impl From<QueryError> for Failure {
    fn from(e: QueryError) -> Self {
        let status = match e.code() {
            1045 | 3118 | 1820 | 1862 | 3159 => StatusCode::UNAUTHORIZED,
            1044 | 1142 | 1143 | 1227 | 1370 => StatusCode::FORBIDDEN,
            1062 | 1213 | 1205 => StatusCode::CONFLICT,
            1226 | 1203 => StatusCode::TOO_MANY_REQUESTS,
            1040 | 1037 | 1038 => StatusCode::SERVICE_UNAVAILABLE,
            3024 => StatusCode::GATEWAY_TIMEOUT,
            1317 => StatusCode::CONFLICT,
            1105 => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::UNPROCESSABLE_ENTITY,
        };
        let mut end = e.message().len().min(2048);
        while !e.message().is_char_boundary(end) {
            end -= 1;
        }
        let mut failure = Self::new(
            status,
            e.symbol().unwrap_or("SQL_ERROR"),
            &e.message()[..end],
        );
        failure.message_truncated = end < e.message().len();
        failure.mysql_errno = Some(e.code());
        failure.sqlstate = Some(e.sql_state().as_str().into());
        failure
    }
}
impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        let mut r = (
            self.status,
            Json(json!({"error":self.value(),"execution_outcome":self.execution_outcome,"request_id":self.request_id})),
        )
            .into_response();
        r.headers_mut().insert(
            "cache-control",
            axum::http::HeaderValue::from_static("no-store"),
        );
        if let Some(id) = &self.request_id {
            if let Ok(value) = axum::http::HeaderValue::from_str(id) {
                r.headers_mut().insert("x-request-id", value);
            }
        }
        if self.status == StatusCode::UNAUTHORIZED {
            r.headers_mut().insert(
                "www-authenticate",
                axum::http::HeaderValue::from_static("Bearer realm=\"mydb\""),
            );
        }
        r
    }
}
