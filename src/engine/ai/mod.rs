//! Bounded external model execution and encrypted catalog credentials.
pub(crate) mod admission;
pub(crate) mod catalog_faults;
mod config;
pub(crate) mod crypto;
pub(crate) mod metadata;
pub(crate) mod runtime;
pub(crate) mod sql;
use crate::types::QueryError;
pub(crate) use config::*;
pub(crate) use crypto::{decrypt_secret, encrypt_secret, load_keys};
pub(crate) use runtime::{call, is_function, with_calls};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) fn error(code: &str, message: impl AsRef<str>) -> QueryError {
    let spec = match code {
        "ER_AI_KEY_ROTATION_INCOMPLETE" => crate::error::catalog::ER_AI_KEY_ROTATION_INCOMPLETE,
        "ER_AI_CIRCUIT_OPEN" => crate::error::catalog::ER_AI_CIRCUIT_OPEN,
        "ER_AI_ACCESS_DENIED" => crate::error::catalog::ER_AI_ACCESS_DENIED,
        "ER_AI_BUDGET_EXCEEDED" => crate::error::catalog::ER_AI_BUDGET_EXCEEDED,
        "ER_AI_CREDENTIAL_UNAVAILABLE" => crate::error::catalog::ER_AI_CREDENTIAL_UNAVAILABLE,
        "ER_AI_DIMENSION_MISMATCH" => crate::error::catalog::ER_AI_DIMENSION_MISMATCH,
        "ER_AI_DISABLED" => crate::error::catalog::ER_AI_DISABLED,
        "ER_AI_EGRESS_DENIED" => crate::error::catalog::ER_AI_EGRESS_DENIED,
        "ER_AI_ENCRYPTION_KEY_INVALID" => crate::error::catalog::ER_AI_ENCRYPTION_KEY_INVALID,
        "ER_AI_ENCRYPTION_KEY_MISSING" => crate::error::catalog::ER_AI_ENCRYPTION_KEY_MISSING,
        "ER_AI_INCOMPLETE" => crate::error::catalog::ER_AI_INCOMPLETE,
        "ER_AI_INVALID_ARGUMENT" => crate::error::catalog::ER_AI_INVALID_ARGUMENT,
        "ER_AI_INVALID_RESPONSE" => crate::error::catalog::ER_AI_INVALID_RESPONSE,
        "ER_AI_MODEL_NOT_FOUND" => crate::error::catalog::ER_AI_MODEL_NOT_FOUND,
        "ER_AI_OUTPUT_SCHEMA" => crate::error::catalog::ER_AI_OUTPUT_SCHEMA,
        "ER_AI_QUEUE_FULL" => crate::error::catalog::ER_AI_QUEUE_FULL,
        "ER_AI_REFUSED" => crate::error::catalog::ER_AI_REFUSED,
        "ER_AI_SECRET_DECRYPT_FAILED" => crate::error::catalog::ER_AI_SECRET_DECRYPT_FAILED,
        "ER_AI_TIMEOUT" => crate::error::catalog::ER_AI_TIMEOUT,
        "ER_AI_UNSUPPORTED_CONTEXT" => crate::error::catalog::ER_AI_UNSUPPORTED_CONTEXT,
        "ER_AI_UPSTREAM_" => crate::error::catalog::ER_AI_UPSTREAM_,
        "ER_AI_UPSTREAM_AUTH" => crate::error::catalog::ER_AI_UPSTREAM_AUTH,
        "ER_AI_UPSTREAM_ERROR" => crate::error::catalog::ER_AI_UPSTREAM_ERROR,
        "ER_AI_WRONG_MODEL_KIND" => crate::error::catalog::ER_AI_WRONG_MODEL_KIND,
        _ => crate::error::catalog::ER_UNKNOWN_ERROR,
    };
    spec.error(format!("{code}: {}", message.as_ref()))
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AiCatalog {
    #[serde(skip)]
    _charge: Option<std::sync::Arc<crate::engine::executor::mem_budget::ControlledMemoryCharge>>,
    pub next_id: u64,
    pub encryption_id: String,
    pub active_key_id: String,
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub encryption_key_ids: std::collections::BTreeSet<String>,
    pub secrets: BTreeMap<String, Secret>,
    pub connections: BTreeMap<String, Connection>,
    pub models: BTreeMap<String, Model>,
    pub grants: BTreeMap<String, Vec<u64>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Secret {
    pub id: u64,
    pub generation: u64,
    #[serde(default)]
    pub crypto_generation: u64,
    pub key_id: String,
    pub wrapped_dek: Vec<u8>,
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
    pub format_version: u32,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Connection {
    #[serde(default)]
    pub id: u64,
    #[serde(default)]
    pub generation: u64,
    pub protocol: String,
    pub base_url: String,
    pub auth: Auth,
    #[serde(default)]
    pub transport: Transport,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default = "yes")]
    pub enabled: bool,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub limits: Limits,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Transport {
    pub connect_timeout_ms: Option<u64>,
    pub ca_file: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub(crate) enum Auth {
    None,
    Bearer { secret: String },
    Header { secret: String, header_name: String },
}
impl Auth {
    pub fn secret(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Bearer { secret } | Self::Header { secret, .. } => Some(secret),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Model {
    #[serde(default)]
    pub id: u64,
    #[serde(default)]
    pub connection: String,
    #[serde(default)]
    pub generation: u64,
    pub kind: String,
    pub remote_model: String,
    pub model_revision: String,
    #[serde(default = "yes")]
    pub enabled: bool,
    #[serde(default)]
    pub defaults: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub capabilities: BTreeMap<String, bool>,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub embedding: Option<Embedding>,
    #[serde(default)]
    pub provider_options_allowed: Vec<String>,
    #[serde(default)]
    pub provider_options_schema: BTreeMap<String, serde_json::Value>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Embedding {
    pub dimensions: usize,
    #[serde(default)]
    pub request_dimensions: bool,
    #[serde(default = "normalization")]
    pub normalize: String,
    #[serde(default)]
    pub space_options: BTreeMap<String, serde_json::Value>,
}
fn normalization() -> String {
    "none".into()
}
fn yes() -> bool {
    true
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Limits {
    pub max_inflight: Option<usize>,
    pub requests_per_minute: Option<u64>,
    pub tokens_per_minute: Option<u64>,
    pub max_input_bytes: Option<usize>,
    pub max_input_tokens: Option<usize>,
    pub max_output_tokens: Option<u64>,
    pub max_batch_items: Option<usize>,
    pub max_retries: Option<u64>,
    #[serde(default)]
    pub allow_retry_ambiguous: bool,
}

#[cfg(test)]
mod tests;

impl AiCatalog {
    pub(crate) fn install_charge(&mut self) -> Result<(), QueryError> {
        let bytes = serde_json::to_vec(self)
            .map_err(|_| error("ER_AI_INVALID_ARGUMENT", "invalid AI catalog"))?
            .len()
            .saturating_mul(16);
        self._charge = Some(std::sync::Arc::new(
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                bytes,
            )?,
        ));
        Ok(())
    }
}
