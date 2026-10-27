use super::*;
use crate::{
    engine::{
        api::SessionContext,
        auth::{Privilege, PrivilegeScope, RequiredPrivilege},
    },
    storage::Database,
    types::Value,
};
use serde_json::{json, Value as Json};
use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, Instant},
};
use zeroize::Zeroizing;

pub(crate) fn is_function(name: &str) -> bool {
    [
        "AI_GENERATE",
        "AI_GENERATE_DETAIL",
        "AI_EMBED",
        "AI_EMBED_DETAIL",
    ]
    .iter()
    .any(|n| name.eq_ignore_ascii_case(n))
}
pub(crate) struct CallScope {
    db: Arc<Database>,
    database: String,
    auth: crate::engine::auth::AuthorizationContext,
    deadline: Instant,
    items: usize,
    attempts: usize,
    catalog: Arc<AiCatalog>,
    control: Arc<DatabaseControl>,
    outputs: Vec<Arc<super::admission::Memory>>,
}
thread_local! { static CALLS:RefCell<Option<CallScope>>=const{RefCell::new(None)}; static LAST:std::cell::Cell<(usize,usize)>=const{std::cell::Cell::new((0,0))}; }
pub(crate) fn prepare_calls(
    db: Arc<Database>,
    session: &SessionContext,
) -> Result<CallScope, QueryError> {
    validate_runtime_limits()?;
    let catalog = db.ai_catalog()?;
    let control = database_control(&db)?;
    if control.cancelled.load(Ordering::Acquire) {
        return Err(error("ER_AI_DISABLED", "AI execution is shutting down"));
    }
    control.active.fetch_add(1, Ordering::AcqRel);
    if control.cancelled.load(Ordering::Acquire) {
        control.active.fetch_sub(1, Ordering::AcqRel);
        return Err(error("ER_AI_DISABLED", "AI execution is shutting down"));
    }
    Ok(CallScope {
        db,
        database: session.current_database.to_ascii_lowercase(),
        auth: session.authorization_context(),
        deadline: Instant::now() + Duration::from_secs(60),
        items: 0,
        attempts: 0,
        catalog,
        control,
        outputs: Vec::new(),
    })
}
impl CallScope {
    pub(crate) fn database(&self) -> &str {
        &self.database
    }
    pub(crate) fn catalog(&self) -> &Arc<AiCatalog> {
        &self.catalog
    }
}
pub(crate) fn with_saved_calls<R>(
    target: &mut Option<CallScope>,
    f: impl FnOnce() -> Result<R, QueryError>,
) -> Result<R, QueryError> {
    struct Restore<'a> {
        target: &'a mut Option<CallScope>,
        previous: Option<CallScope>,
    }
    impl Drop for Restore<'_> {
        fn drop(&mut self) {
            let mut completed = CALLS.with(|s| s.replace(self.previous.take()));
            if let Some(scope) = completed.as_mut() {
                scope.outputs.clear();
                LAST.with(|last| last.set((scope.items, scope.attempts)));
            }
            *self.target = completed;
        }
    }
    let scope = target.take().ok_or_else(|| {
        error(
            "ER_AI_UNSUPPORTED_CONTEXT",
            "missing external execution scope",
        )
    })?;
    let previous = CALLS.with(|s| s.replace(Some(scope)));
    let _restore = Restore { target, previous };
    f()
}
pub(crate) fn with_calls<R>(
    db: Arc<Database>,
    session: &SessionContext,
    f: impl FnOnce() -> Result<R, QueryError>,
) -> Result<R, QueryError> {
    let mut scope = Some(prepare_calls(db, session)?);
    with_saved_calls(&mut scope, f)
}
pub(crate) fn authorize(
    db: &Database,
    ctx: &crate::engine::auth::AuthorizationContext,
    privilege: &str,
) -> Result<(), QueryError> {
    let required = RequiredPrivilege {
        privilege: Privilege::Dynamic(privilege.into()),
        scope: PrivilegeScope::Global,
    };
    if !db
        .auth_catalog()
        .account_has_privilege(ctx, &required, false)
    {
        return Err(error("ER_AI_ACCESS_DENIED", "AI privilege required"));
    }
    Ok(())
}
fn as_json(value: &Value) -> Result<Json, QueryError> {
    match value {
        Value::Json(v) => {
            if v.len() > 65536 {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    "options JSON exceeds 64 KiB",
                ));
            }
            let json = crate::engine::json::to_serde(v)?;
            validate_json_depth(&json, 0)?;
            Ok(json)
        }
        Value::SmallString(_) => parse_json(crate::types::value_str(value).unwrap(), 65536),
        Value::String(s) => parse_json(s, 65536),
        Value::Null => Ok(json!({})),
        _ => Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "options require a JSON object",
        )),
    }
}
fn validate_json_depth(value: &Json, depth: usize) -> Result<(), QueryError> {
    if matches!(value, Json::Array(_) | Json::Object(_)) && depth >= 32 {
        return Err(error("ER_AI_INVALID_ARGUMENT", "JSON nesting exceeds 32"));
    }
    match value {
        Json::Array(items) => {
            for item in items {
                validate_json_depth(item, depth + 1)?;
            }
        }
        Json::Object(items) => {
            for item in items.values() {
                validate_json_depth(item, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn text(value: &Value) -> Result<String, QueryError> {
    crate::types::value_str(value)
        .map(str::to_owned)
        .ok_or_else(|| error("ER_AI_INVALID_ARGUMENT", "input must be text"))
}
fn merge(base: &mut Json, patch: Json) {
    match (base, patch) {
        (Json::Object(a), Json::Object(b)) => {
            for (key, value) in b {
                if let Some(old) = a.get_mut(&key) {
                    merge(old, value);
                } else {
                    a.insert(key, value);
                }
            }
        }
        (a, b) => *a = b,
    }
}
fn merge_options(base: &mut Json, patch: Json) {
    if let (Some(a), Json::Object(b)) = (base.as_object_mut(), patch) {
        for (key, value) in b {
            if value.is_null() {
                a.remove(&key);
            } else if let Some(old) = a.get_mut(&key) {
                merge(old, value);
            } else {
                a.insert(key, value);
            }
        }
    }
}
fn positive(o: &Json, name: &str, default: u64, max: u64) -> Result<u64, QueryError> {
    match o.get(name) {
        None => Ok(default.min(max)),
        Some(v) => v
            .as_u64()
            .filter(|n| *n > 0 && *n <= max)
            .ok_or_else(|| error("ER_AI_INVALID_ARGUMENT", format!("invalid {name}"))),
    }
}
fn validate_options(model: &Model, options: &Json) -> Result<(), QueryError> {
    let map = options
        .as_object()
        .ok_or_else(|| error("ER_AI_INVALID_ARGUMENT", "options must be an object"))?;
    for (k, v) in map {
        match k.as_str() {
            "temperature" | "top_p" => {
                if model.kind != "text_generation"
                    || !model.capabilities.get(k).copied().unwrap_or(false)
                {
                    return Err(error(
                        "ER_AI_INVALID_ARGUMENT",
                        format!("model does not support {k}"),
                    ));
                }
                let n = v
                    .as_f64()
                    .filter(|n| n.is_finite())
                    .ok_or_else(|| error("ER_AI_INVALID_ARGUMENT", "numeric parameter required"))?;
                if (k == "temperature" && !(0.0..=2.0).contains(&n))
                    || (k == "top_p" && !(n > 0.0 && n <= 1.0))
                {
                    return Err(error(
                        "ER_AI_INVALID_ARGUMENT",
                        "sampling parameter out of range",
                    ));
                }
            }
            "stop" => {
                if !model.capabilities.get(k).copied().unwrap_or(false) {
                    return Err(error("ER_AI_INVALID_ARGUMENT", "stop is unsupported"));
                }
                let valid = |s: &str| !s.is_empty() && s.len() <= 1024;
                let ok = if let Some(s) = v.as_str() {
                    valid(s)
                } else {
                    v.as_array().is_some_and(|a| {
                        !a.is_empty()
                            && a.len() <= 4
                            && a.iter().all(|v| v.as_str().is_some_and(valid))
                    })
                };
                if !ok {
                    return Err(error("ER_AI_INVALID_ARGUMENT", "invalid stop strings"));
                }
            }
            "seed" => {
                if !model.capabilities.get(k).copied().unwrap_or(false) || v.as_i64().is_none() {
                    return Err(error(
                        "ER_AI_INVALID_ARGUMENT",
                        "seed is unsupported or invalid",
                    ));
                }
            }
            "max_output_tokens" => {
                if model.kind != "text_generation" {
                    return Err(error(
                        "ER_AI_INVALID_ARGUMENT",
                        "embedding does not accept generation options",
                    ));
                }
                positive(
                    options,
                    k,
                    1024,
                    model.limits.max_output_tokens.unwrap_or(4096).min(4096),
                )?;
            }
            "timeout_ms" => {
                positive(options, k, 30000, 60000)?;
            }
            "max_retries" => {
                if v.as_u64()
                    .is_none_or(|n| n > model.limits.max_retries.unwrap_or(0).min(8))
                {
                    return Err(error(
                        "ER_AI_INVALID_ARGUMENT",
                        "max_retries exceeds model policy",
                    ));
                }
            }
            "retry_ambiguous" => {
                let value = v.as_bool().ok_or_else(|| {
                    error("ER_AI_INVALID_ARGUMENT", "retry_ambiguous must be boolean")
                })?;
                if value && !model.limits.allow_retry_ambiguous {
                    return Err(error(
                        "ER_AI_INVALID_ARGUMENT",
                        "ambiguous retries are prohibited by model policy",
                    ));
                }
            }
            "on_error" => {
                if !matches!(v.as_str(), Some("error" | "return")) {
                    return Err(error("ER_AI_INVALID_ARGUMENT", "invalid on_error"));
                }
            }
            "response_format" => {
                if model.kind != "text_generation" {
                    return Err(error(
                        "ER_AI_INVALID_ARGUMENT",
                        "embedding does not accept response_format",
                    ));
                }
                let obj = v
                    .as_object()
                    .ok_or_else(|| error("ER_AI_INVALID_ARGUMENT", "invalid response_format"))?;
                match v.get("type").and_then(Json::as_str) {
                    Some("text") => {
                        if obj.len() != 1 {
                            return Err(error(
                                "ER_AI_INVALID_ARGUMENT",
                                "unknown text format field",
                            ));
                        }
                    }
                    Some("json_schema") => {
                        if !model
                            .capabilities
                            .get("json_schema")
                            .copied()
                            .unwrap_or(false)
                            || obj.keys().any(|k| {
                                !matches!(k.as_str(), "type" | "name" | "schema" | "strict")
                            })
                            || v.get("strict") != Some(&Json::Bool(true))
                            || !v.get("name").and_then(Json::as_str).is_some_and(|name| {
                                !name.is_empty()
                                    && name.len() <= 64
                                    && name.bytes().all(|c| {
                                        c.is_ascii_alphanumeric() || c == b'_' || c == b'-'
                                    })
                            })
                            || v.get("schema").is_none()
                        {
                            return Err(error(
                                "ER_AI_INVALID_ARGUMENT",
                                "invalid or unsupported JSON schema format",
                            ));
                        }
                        let schema = &v["schema"];
                        if schema.to_string().len() > 32768
                            || schema["type"] != "object"
                            || contains_ref(schema)
                        {
                            return Err(error(
                                "ER_AI_INVALID_ARGUMENT",
                                "unsupported output schema",
                            ));
                        }
                        jsonschema::validator_for(schema).map_err(|_| {
                            error("ER_AI_INVALID_ARGUMENT", "invalid output schema")
                        })?;
                    }
                    _ => return Err(error("ER_AI_INVALID_ARGUMENT", "unknown response_format")),
                }
            }
            "provider_options" => {
                let mut paths = Vec::new();
                flatten(v, "", &mut paths)?;
                for (path, value) in paths {
                    if !model.provider_options_allowed.contains(&path) {
                        return Err(error(
                            "ER_AI_INVALID_ARGUMENT",
                            format!("provider option {path} is not allowed"),
                        ));
                    }
                    let built_in = match path.as_str() {
                        "think" => value.is_boolean(),
                        "reasoning.effort" => value.as_str().is_some_and(|s| {
                            matches!(s, "none" | "minimal" | "low" | "medium" | "high" | "xhigh")
                        }),
                        _ => false,
                    };
                    if let Some(schema) = model.provider_options_schema.get(&path) {
                        if contains_ref(schema) || !jsonschema::is_valid(schema, value) {
                            return Err(error(
                                "ER_AI_INVALID_ARGUMENT",
                                "provider option schema mismatch",
                            ));
                        }
                    } else if !built_in {
                        return Err(error(
                            "ER_AI_INVALID_ARGUMENT",
                            "provider option requires an explicit schema",
                        ));
                    }
                    if path.split('.').any(|p| {
                        matches!(
                            p,
                            "model"
                                | "input"
                                | "messages"
                                | "stream"
                                | "tools"
                                | "tool_choice"
                                | "endpoint"
                                | "auth"
                                | "headers"
                                | "conversation"
                                | "previous_response_id"
                                | "background"
                                | "store"
                                | "max_tokens"
                                | "max_completion_tokens"
                                | "max_output_tokens"
                                | "num_predict"
                                | "temperature"
                                | "top_p"
                                | "stop"
                                | "seed"
                                | "response_format"
                                | "format"
                                | "text"
                                | "url"
                                | "image_url"
                                | "file_id"
                                | "modalities"
                                | "encoding_format"
                                | "truncate"
                                | "n"
                                | "timeout_ms"
                                | "max_retries"
                                | "retry_ambiguous"
                                | "on_error"
                                | "dimensions"
                                | "normalize"
                                | "datatype"
                                | "task_type"
                        )
                    }) {
                        return Err(error("ER_AI_INVALID_ARGUMENT", "reserved provider option"));
                    }
                }
            }
            _ => {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    format!("unknown option {k}"),
                ))
            }
        }
    }
    Ok(())
}
fn contains_ref(v: &Json) -> bool {
    match v {
        Json::Object(m) => m.iter().any(|(k, v)| k == "$ref" || contains_ref(v)),
        Json::Array(a) => a.iter().any(contains_ref),
        _ => false,
    }
}
fn flatten<'a>(
    v: &'a Json,
    prefix: &str,
    out: &mut Vec<(String, &'a Json)>,
) -> Result<(), QueryError> {
    let obj = v.as_object().ok_or_else(|| {
        error(
            "ER_AI_INVALID_ARGUMENT",
            "provider_options must be an object",
        )
    })?;
    for (k, v) in obj {
        let path = if prefix.is_empty() {
            k.clone()
        } else {
            format!("{prefix}.{k}")
        };
        if v.is_object() {
            flatten(v, &path, out)?;
        } else {
            out.push((path, v));
        }
    }
    Ok(())
}
fn input_json(
    value: &Value,
    embedding: bool,
    detail: bool,
    model: &Model,
) -> Result<(Json, usize), QueryError> {
    let max_input = model.limits.max_input_bytes.unwrap_or(262144).min(262144);
    if let Value::Json(v) = value {
        let json_limit = if embedding && detail {
            max_input
                .saturating_add(64)
                .saturating_mul(model.limits.max_batch_items.unwrap_or(64).min(64))
                .min(8 * 1024 * 1024)
        } else {
            max_input.saturating_add(65536)
        };
        if v.len() > json_limit {
            return Err(error(
                "ER_AI_INVALID_ARGUMENT",
                "JSON input exceeds byte limit",
            ));
        }
        let j = crate::engine::json::to_serde(v)?;
        validate_json_depth(&j, 0)?;
        let a = j
            .as_array()
            .ok_or_else(|| error("ER_AI_INVALID_ARGUMENT", "input JSON must be an array"))?;
        if embedding {
            if !detail
                || a.is_empty()
                || a.len() > model.limits.max_batch_items.unwrap_or(64).min(64)
                || a.iter().any(|v| {
                    v.as_str()
                        .is_none_or(|text| text.is_empty() || text.len() > max_input)
                })
            {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    "invalid embedding text array",
                ));
            }
            let count = a.len();
            return Ok((j, count));
        }
        if !model.capabilities.get("messages").copied().unwrap_or(false) || a.is_empty() {
            return Err(error(
                "ER_AI_INVALID_ARGUMENT",
                "model does not support messages",
            ));
        }
        let mut user = false;
        for m in a {
            let obj = m
                .as_object()
                .ok_or_else(|| error("ER_AI_INVALID_ARGUMENT", "invalid message"))?;
            if obj.len() != 2
                || m["content"].as_str().is_none()
                || !matches!(m["role"].as_str(), Some("user" | "system" | "assistant"))
            {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    "only text system/user/assistant messages are supported",
                ));
            }
            user |= m["role"] == "user";
        }
        if !user {
            return Err(error(
                "ER_AI_INVALID_ARGUMENT",
                "a user message is required",
            ));
        }
        Ok((j, 1))
    } else {
        let s = crate::types::value_str(value)
            .ok_or_else(|| error("ER_AI_INVALID_ARGUMENT", "input must be text"))?;
        if s.len() > max_input {
            return Err(error("ER_AI_INVALID_ARGUMENT", "input exceeds byte limit"));
        }
        if embedding && s.is_empty() {
            return Err(error(
                "ER_AI_INVALID_ARGUMENT",
                "embedding input cannot be empty",
            ));
        }
        Ok((Json::String(s.to_owned()), 1))
    }
}
fn request_body(
    model: &Model,
    connection: &Connection,
    input: &Json,
    options: &Json,
) -> Result<(String, Json), QueryError> {
    let embed = model.kind == "embedding";
    let protocol = connection.protocol.as_str();
    let mut body = json!({"model":model.remote_model});
    let path = if embed {
        body["input"] = input.clone();
        let e = model.embedding.as_ref().unwrap();
        if e.request_dimensions {
            body["dimensions"] = json!(e.dimensions);
        }
        if protocol == "ollama" {
            body["truncate"] = json!(false);
            "/api/embed"
        } else {
            body["encoding_format"] = json!("float");
            "/embeddings"
        }
    } else {
        body["stream"] = json!(false);
        let messages = if input.is_string() {
            json!([{"role":"user","content":input}])
        } else {
            input.clone()
        };
        match protocol {
            "ollama" => {
                body["messages"] = messages;
                body["options"] = json!({});
                "/api/chat"
            }
            "openai_chat" => {
                body["messages"] = messages;
                body["n"] = json!(1);
                body["store"] = json!(false);
                "/chat/completions"
            }
            "openai_responses" => {
                body["input"] = messages;
                body["store"] = json!(false);
                "/responses"
            }
            _ => {
                return Err(error(
                    "ER_AI_WRONG_MODEL_KIND",
                    "connection does not support text generation",
                ))
            }
        }
    };
    for k in ["temperature", "top_p", "stop", "seed"] {
        if let Some(v) = options.get(k) {
            if protocol == "ollama" {
                body["options"][k] = v.clone();
            } else {
                body[k] = v.clone();
            }
        }
    }
    if !embed {
        let max = positive(
            options,
            "max_output_tokens",
            1024,
            model.limits.max_output_tokens.unwrap_or(4096).min(4096),
        )?;
        match protocol {
            "ollama" => body["options"]["num_predict"] = json!(max),
            "openai_chat" => {
                body[if connection.profile.as_deref() == Some("openai_chat_legacy") {
                    "max_tokens"
                } else {
                    "max_completion_tokens"
                }] = json!(max)
            }
            _ => body["max_output_tokens"] = json!(max),
        };
    }
    if let Some(format) = options.get("response_format") {
        if format["type"] == "json_schema" {
            match protocol {
                "ollama" => body["format"] = format["schema"].clone(),
                "openai_chat" => {
                    body["response_format"] = json!({"type":"json_schema","json_schema":{"name":format["name"],"strict":true,"schema":format["schema"]}})
                }
                _ => body["text"] = json!({"format":format}),
            }
        }
    }
    if let Some(extra) = options.get("provider_options") {
        merge(&mut body, extra.clone());
    }
    Ok((
        format!("{}{path}", connection.base_url.trim_end_matches('/')),
        body,
    ))
}
#[derive(Clone)]
struct Dispatch {
    db: Arc<Database>,
    auth: crate::engine::auth::AuthorizationContext,
    model_name: String,
    model: Model,
    connection_id: u64,
    connection_generation: u64,
    namespace: String,
    principal: String,
    user: Option<UserLimit>,
    local: LocalConfig,
    breaker_key: String,
    breaker_revision: (u64, u64, u64),
    secret: Option<(String, u64, u64)>,
    control: Arc<DatabaseControl>,
}
impl Dispatch {
    fn live_catalog(&self) -> Result<Arc<AiCatalog>, QueryError> {
        if self.control.cancelled.load(Ordering::Acquire) {
            return Err(error("ER_AI_DISABLED", "AI execution is shutting down"));
        }
        if !dispatch_enabled() {
            return Err(error("ER_AI_DISABLED", "AI calls are disabled"));
        }
        if !self.local.generation_is_current()? {
            return Err(error(
                "ER_AI_EGRESS_DENIED",
                "local AI configuration changed; retry the statement",
            ));
        }
        let state = self.db.ai_catalog()?;
        if self.secret.as_ref().is_some_and(|(name, id, generation)| {
            !state
                .secrets
                .get(name)
                .is_some_and(|s| s.id == *id && s.generation == *generation)
        }) {
            return Err(error(
                "ER_AI_CREDENTIAL_UNAVAILABLE",
                "credential was rotated or dropped; retry the statement",
            ));
        }
        let model = state
            .models
            .get(&self.model_name)
            .filter(|m| m.id == self.model.id && m.enabled)
            .ok_or_else(|| error("ER_AI_DISABLED", "model was disabled or dropped"))?;
        if !state.connections.get(&model.connection).is_some_and(|c| {
            c.enabled && c.id == self.connection_id && c.generation == self.connection_generation
        }) {
            return Err(error(
                "ER_AI_DISABLED",
                "connection was disabled, dropped or reconfigured; retry the statement",
            ));
        }
        if authorize(&self.db, &self.auth, "AI_USE").is_err() {
            authorize(&self.db, &self.auth, &format!("AI_MODEL_{}_USE", model.id))?;
        }
        Ok(state)
    }
    fn check(&self) -> Result<(), QueryError> {
        self.live_catalog().map(|_| ())
    }
    fn quotas(&self, instance_capacity: usize) -> Result<Vec<super::admission::Scope>, QueryError> {
        let state = self.live_catalog()?;
        let model = &state.models[&self.model_name];
        let connection = &state.connections[&model.connection];
        Ok(super::admission::scopes(
            &self.namespace,
            &self.principal,
            connection,
            model,
            self.user.as_ref(),
            self.local.generation,
            instance_capacity,
        ))
    }
}
struct Runtime {
    rt: tokio::runtime::Runtime,
    max_inflight: usize,
    clients: tokio::sync::Mutex<HashMap<String, (Instant, reqwest::Client)>>,
}
static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();
fn runtime() -> Result<&'static Runtime, QueryError> {
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("mydb-ai-io")
                .enable_all()
                .build()
                .map(|rt| Runtime {
                    rt,
                    max_inflight: env_limit("MYDB_AI_MAX_INFLIGHT", 32).min(256),
                    clients: tokio::sync::Mutex::new(HashMap::new()),
                })
                .map_err(|_| "cannot start AI runtime".into())
        })
        .as_ref()
        .map_err(|e| error("ER_AI_DISABLED", e))
}
static PENDING: AtomicUsize = AtomicUsize::new(0);
static TOTAL_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static FAILED_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static HTTP_ATTEMPTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
struct Pending {
    control: Arc<DatabaseControl>,
}
impl Drop for Pending {
    fn drop(&mut self) {
        PENDING.fetch_sub(1, Ordering::AcqRel);
        if self.control.pending.fetch_sub(1, Ordering::AcqRel) == 1
            && self.control.cancelled.load(Ordering::Acquire)
        {
            retire_runtime_namespace(&self.control.namespace);
        }
    }
}
struct ProviderReply {
    body: Json,
    request_id: Option<String>,
    queue: Duration,
    upstream: Duration,
}
struct ProviderFailure {
    error: QueryError,
    retryable: bool,
    ambiguous: bool,
    status: Option<u16>,
    retry_after: Option<Duration>,
}
impl From<QueryError> for ProviderFailure {
    fn from(error: QueryError) -> Self {
        let ambiguous = error.is(crate::error::catalog::ER_AI_TIMEOUT);
        Self {
            error,
            retryable: false,
            ambiguous,
            status: None,
            retry_after: None,
        }
    }
}
async fn http(
    runtime: &Runtime,
    local: LocalConfig,
    connection: Connection,
    url: String,
    body: Json,
    credential: Option<Zeroizing<Vec<u8>>>,
    dispatch: Dispatch,
    response_limit: usize,
    memory: Arc<super::admission::Memory>,
) -> Result<ProviderReply, ProviderFailure> {
    let queued = Instant::now();
    let breaker =
        super::admission::breaker(dispatch.breaker_key.clone(), dispatch.breaker_revision)?;
    let mut quotas = super::admission::acquire(|| dispatch.quotas(runtime.max_inflight)).await?;
    let outcome = http_inner(
        runtime,
        local,
        connection,
        url,
        body,
        credential,
        dispatch,
        response_limit,
        &memory,
        queued,
        &breaker,
        &mut quotas,
    )
    .await;
    match &outcome {
        Ok(_) => breaker.record(true, false, false),
        Err(e) => breaker.record(
            false,
            e.retryable && e.status != Some(429),
            e.status.is_some_and(|s| s == 401 || s == 403),
        ),
    };
    outcome
}
async fn http_inner(
    runtime: &Runtime,
    local: LocalConfig,
    connection: Connection,
    url: String,
    body: Json,
    credential: Option<Zeroizing<Vec<u8>>>,
    dispatch: Dispatch,
    response_limit: usize,
    memory: &super::admission::Memory,
    queued: Instant,
    breaker: &super::admission::BreakerPermit,
    quotas: &mut super::admission::Permit,
) -> Result<ProviderReply, ProviderFailure> {
    dispatch.check()?;
    let local = local.current()?;
    let base = local.check_url(&connection.base_url)?;
    let host = base
        .host_str()
        .ok_or_else(|| error("ER_AI_EGRESS_DENIED", "missing hostname"))?;
    // URL IPv6 literals include brackets; socket resolution expects the bare IP.
    let host = host.trim_matches(['[', ']']);
    let cache_key = format!(
        "{}:{}:{}:{}",
        dispatch.namespace,
        connection.id,
        connection.generation,
        format!(
            "{}:{:?}",
            format!("{}:{}", connection.base_url, local.generation),
            serde_json::to_string(&local.egress)
        )
    );
    let cached = runtime
        .clients
        .lock()
        .await
        .get(&cache_key)
        .filter(|(t, _)| t.elapsed() < Duration::from_secs(60))
        .map(|(_, client)| client.clone());
    let client = if let Some(client) = cached {
        client
    } else {
        let addrs: Vec<_> =
            tokio::net::lookup_host((host, base.port_or_known_default().unwrap_or(443)))
                .await
                .map_err(|_| ProviderFailure {
                    error: error(
                        "ER_AI_UPSTREAM_ERROR",
                        "DNS resolution failed before dispatch",
                    ),
                    retryable: true,
                    ambiguous: false,
                    status: None,
                    retry_after: None,
                })?
                .collect();
        if addrs.is_empty() {
            return Err(error("ER_AI_EGRESS_DENIED", "DNS returned no addresses").into());
        }
        for a in &addrs {
            local.check_ip(a.ip())?;
        }
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_millis(
                connection.transport.connect_timeout_ms.unwrap_or(5000),
            ))
            .resolve_to_addrs(host, &addrs)
            .pool_max_idle_per_host(16);
        if let Some(name) = &connection.transport.ca_file {
            let path = local
                .ca_bindings
                .get(name)
                .ok_or_else(|| error("ER_AI_INVALID_ARGUMENT", "CA binding unavailable"))?;
            let bytes = read_bounded(std::path::Path::new(path), 1024 * 1024)?;
            for cert in reqwest::Certificate::from_pem_bundle(&bytes)
                .map_err(|_| error("ER_AI_INVALID_ARGUMENT", "invalid CA bundle"))?
            {
                builder = builder.add_root_certificate(cert);
            }
        }
        let client = builder
            .build()
            .map_err(|_| error("ER_AI_UPSTREAM_ERROR", "cannot initialize HTTP client"))?;
        let mut clients = runtime.clients.lock().await;
        if clients.len() >= 128 {
            clients.retain(|_, (t, _)| t.elapsed() < Duration::from_secs(60));
            if clients.len() >= 128 {
                clients.clear();
            }
        }
        clients.insert(cache_key, (Instant::now(), client.clone()));
        client
    };
    let mut request = client.post(url).json(&body);
    for (k, v) in &connection.headers {
        request = request.header(k, v);
    }
    if let Some(secret) = credential {
        let value = std::str::from_utf8(&secret).map_err(|_| {
            error(
                "ER_AI_CREDENTIAL_UNAVAILABLE",
                "invalid credential encoding",
            )
        })?;
        let (name, value) = match &connection.auth {
            Auth::Bearer { .. } => ("authorization", format!("Bearer {value}")),
            Auth::Header { header_name, .. } => (header_name.as_str(), value.to_owned()),
            Auth::None => {
                return Err(error("ER_AI_CREDENTIAL_UNAVAILABLE", "unexpected credential").into())
            }
        };
        let mut h = reqwest::header::HeaderValue::from_str(&value)
            .map_err(|_| error("ER_AI_CREDENTIAL_UNAVAILABLE", "invalid credential header"))?;
        h.set_sensitive(true);
        request = request.header(name, h);
    }
    dispatch.check()?;
    breaker.check()?;
    quotas.mark_dispatched();
    HTTP_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
    let upstream_start = Instant::now();
    let mut response = request.send().await.map_err(|e| ProviderFailure {
        error: error("ER_AI_UPSTREAM_ERROR", "HTTP transport failed"),
        retryable: !e.is_builder(),
        ambiguous: !e.is_connect() && !e.is_builder(),
        status: None,
        retry_after: None,
    })?;
    let status = response.status();
    let provider_request_id = response
        .headers()
        .get("x-request-id")
        .or_else(|| response.headers().get("request-id"))
        .and_then(|h| h.to_str().ok())
        .filter(|s| s.len() <= 256)
        .map(str::to_owned);
    if !status.is_success() {
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| {
                s.parse::<u64>().ok().map(Duration::from_secs).or_else(|| {
                    httpdate::parse_http_date(s).ok().map(|t| {
                        t.duration_since(std::time::SystemTime::now())
                            .unwrap_or_default()
                    })
                })
            });
        return Err(ProviderFailure {
            error: error(
                if status == 401 || status == 403 {
                    "ER_AI_UPSTREAM_AUTH"
                } else {
                    "ER_AI_UPSTREAM_ERROR"
                },
                format!("provider HTTP status {}", status.as_u16()),
            ),
            retryable: status.is_server_error() || status == 429 || status == 408,
            ambiguous: status.is_server_error() || status == 429 || status == 408,
            status: Some(status.as_u16()),
            retry_after,
        });
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| error("ER_AI_INVALID_RESPONSE", "incomplete response body"))?
    {
        if bytes.len() + chunk.len() > response_limit {
            return Err(error("ER_AI_INVALID_RESPONSE", "response exceeds its byte budget").into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| error("ER_AI_INVALID_RESPONSE", "provider response is not UTF-8"))?;
    let body = parse_json_budgeted(text, response_limit, |bytes| {
        // Generated text may itself be JSON decoded for schema validation. Keep
        // the conservative expansion allowance for that second DOM as well.
        let bytes = if dispatch.model.kind == "embedding" {
            bytes
        } else {
            bytes.max(text.len().saturating_mul(32))
        };
        memory.reserve_response(bytes)
    })
    .map_err(|e| {
        if e.is(crate::error::catalog::ER_AI_INVALID_ARGUMENT) {
            error("ER_AI_INVALID_RESPONSE", "invalid provider JSON")
        } else {
            e
        }
    })?;
    Ok(ProviderReply {
        body,
        request_id: provider_request_id,
        queue: upstream_start.duration_since(queued),
        upstream: upstream_start.elapsed(),
    })
}

struct PendingProvider {
    receiver: std::sync::mpsc::Receiver<Result<ProviderReply, ProviderFailure>>,
    task: tokio::task::JoinHandle<()>,
    dispatch: Dispatch,
    deadline: Instant,
}
impl Drop for PendingProvider {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl PendingProvider {
    fn poll(&mut self) -> Option<Result<ProviderReply, ProviderFailure>> {
        if let Err(e) = crate::engine::cancel::check_cancelled() {
            return Some(Err(e.into()));
        }
        if let Err(e) = self.dispatch.check() {
            return Some(Err(e.into()));
        }
        if Instant::now() >= self.deadline {
            return Some(Err(error(
                "ER_AI_TIMEOUT",
                "AI deadline exceeded; provider may still execute",
            )
            .into()));
        }
        match self.receiver.try_recv() {
            Ok(result) => Some(result),
            Err(std::sync::mpsc::TryRecvError::Empty) => None,
            Err(_) => Some(Err(error("ER_AI_UPSTREAM_ERROR", "AI task stopped").into())),
        }
    }
}
fn start_pending(
    call: &PreparedCall,
    wake: Option<std::sync::mpsc::Sender<()>>,
) -> Result<PendingProvider, ProviderFailure> {
    crate::engine::cancel::check_cancelled()?;
    call.dispatch.check()?;
    if Instant::now() >= call.deadline {
        return Err(error("ER_AI_TIMEOUT", "AI deadline expired before dispatch").into());
    }
    let runtime = runtime()?;
    let limit =
        env_limit("MYDB_AI_MAX_QUEUE", 128).saturating_add(env_limit("MYDB_AI_MAX_INFLIGHT", 32));
    PENDING
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            n.checked_add(1).filter(|v| *v <= limit)
        })
        .map_err(|_| error("ER_AI_QUEUE_FULL", "AI queue is full"))?;
    call.dispatch.control.pending.fetch_add(1, Ordering::AcqRel);
    let pending = Pending {
        control: call.dispatch.control.clone(),
    };
    let (tx, receiver) = std::sync::mpsc::sync_channel(1);
    let local = call.local.clone();
    let connection = call.connection.clone();
    let url = call.url.clone();
    let body = call.body.clone();
    let credential = call.credential.clone();
    let dispatch = call.dispatch.clone();
    let deadline = call.deadline;
    let response_limit = call.response_limit;
    let memory = call.memory.clone();
    let task = runtime.rt.spawn(async move {
        let _pending = pending;
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            http(
                runtime,
                local,
                connection,
                url,
                body,
                credential,
                dispatch,
                response_limit,
                memory,
            ),
        )
        .await
        .unwrap_or_else(|_| {
            Err(error(
                "ER_AI_TIMEOUT",
                "AI deadline exceeded; provider may still execute",
            )
            .into())
        });
        let _ = tx.send(result);
        if let Some(wake) = wake {
            let _ = wake.send(());
        }
    });
    Ok(PendingProvider {
        receiver,
        task,
        dispatch: call.dispatch.clone(),
        deadline,
    })
}
fn send(call: &PreparedCall) -> Result<ProviderReply, ProviderFailure> {
    let (wake_tx, wake_rx) = std::sync::mpsc::channel();
    let mut pending = start_pending(call, Some(wake_tx))?;
    loop {
        if let Some(result) = pending.poll() {
            return result;
        }
        let _ = wake_rx.recv_timeout(Duration::from_millis(10));
    }
}

fn vectors(
    response: &Json,
    connection: &Connection,
    model: &Model,
    count: usize,
) -> Result<Vec<Vec<f32>>, QueryError> {
    let e = model.embedding.as_ref().unwrap();
    let mut ordered = vec![None; count];
    if connection.protocol == "ollama" {
        let a = response["embeddings"]
            .as_array()
            .ok_or_else(|| error("ER_AI_INVALID_RESPONSE", "missing embeddings"))?;
        if a.len() != count {
            return Err(error("ER_AI_INVALID_RESPONSE", "embedding count mismatch"));
        }
        for (i, v) in a.iter().enumerate() {
            ordered[i] = Some(v);
        }
    } else {
        let a = response["data"]
            .as_array()
            .ok_or_else(|| error("ER_AI_INVALID_RESPONSE", "missing embedding data"))?;
        if a.len() != count {
            return Err(error("ER_AI_INVALID_RESPONSE", "embedding count mismatch"));
        }
        for v in a {
            let i = v["index"]
                .as_u64()
                .and_then(|i| usize::try_from(i).ok())
                .filter(|i| *i < count)
                .ok_or_else(|| error("ER_AI_INVALID_RESPONSE", "invalid embedding index"))?;
            if ordered[i].replace(&v["embedding"]).is_some() {
                return Err(error("ER_AI_INVALID_RESPONSE", "duplicate embedding index"));
            }
        }
    }
    let mut out = Vec::with_capacity(count);
    for v in ordered {
        let a = v
            .and_then(Json::as_array)
            .ok_or_else(|| error("ER_AI_INVALID_RESPONSE", "invalid embedding array"))?;
        if a.len() != e.dimensions {
            return Err(error(
                "ER_AI_DIMENSION_MISMATCH",
                "embedding dimension mismatch",
            ));
        }
        let mut vector = Vec::with_capacity(e.dimensions);
        for value in a {
            let x = value.as_f64().filter(|x| x.is_finite()).ok_or_else(|| {
                error(
                    "ER_AI_INVALID_RESPONSE",
                    "embedding contains a non-finite value",
                )
            })? as f32;
            if !x.is_finite() {
                return Err(error(
                    "ER_AI_INVALID_RESPONSE",
                    "embedding overflows FLOAT32",
                ));
            }
            vector.push(x);
        }
        if e.normalize == "l2" {
            let norm = vector
                .iter()
                .map(|v| (*v as f64) * (*v as f64))
                .sum::<f64>()
                .sqrt();
            if norm == 0.0 || !norm.is_finite() {
                return Err(error(
                    "ER_AI_INVALID_RESPONSE",
                    "cannot normalize zero embedding",
                ));
            }
            for v in &mut vector {
                *v = (*v as f64 / norm) as f32;
            }
        }
        out.push(vector);
    }
    Ok(out)
}
fn validate_text_output_message(message: &Json) -> Result<(), QueryError> {
    if message.get("role").and_then(Json::as_str) != Some("assistant") {
        return Err(error(
            "ER_AI_INVALID_RESPONSE",
            "text output must have the assistant role",
        ));
    }
    if !message["function_call"].is_null()
        || (!message["tool_calls"].is_null()
            && !message["tool_calls"].as_array().is_some_and(Vec::is_empty))
    {
        return Err(error(
            "ER_AI_INVALID_RESPONSE",
            "tool and function outputs are disabled",
        ));
    }
    Ok(())
}
fn decode_output(
    response: &Json,
    connection: &Connection,
    model: &Model,
    count: usize,
    options: &Json,
    detail: bool,
) -> Result<(Option<Value>, Json), QueryError> {
    if model.kind == "embedding" {
        let vectors = vectors(response, connection, model, count)?;
        if !detail {
            return Ok((
                Some(Value::Vector(
                    vectors[0]
                        .iter()
                        .flat_map(|v| v.to_le_bytes())
                        .collect::<Vec<_>>()
                        .into(),
                )),
                Json::Null,
            ));
        }
        let embedding = model.embedding.as_ref().unwrap();
        return Ok((
            None,
            json!({"dimensions":embedding.dimensions,"datatype":"FLOAT32","normalization":embedding.normalize,"data":vectors.into_iter().enumerate().map(|(index,embedding)|json!({"index":index,"embedding":embedding})).collect::<Vec<_>>() }),
        ));
    }
    let (text, finish) = match connection.protocol.as_str() {
        "ollama" => {
            if response["done"] != true {
                return Err(error("ER_AI_INCOMPLETE", "provider did not complete"));
            }
            validate_text_output_message(&response["message"])?;
            (
                response["message"]["content"].as_str().map(str::to_owned),
                response["done_reason"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_owned(),
            )
        }
        "openai_chat" => {
            let choices = response["choices"]
                .as_array()
                .filter(|a| a.len() == 1)
                .ok_or_else(|| error("ER_AI_INVALID_RESPONSE", "expected exactly one choice"))?;
            let choice = &choices[0];
            if !choice["message"]["refusal"].is_null() {
                return Err(error("ER_AI_REFUSED", "provider refused"));
            }
            validate_text_output_message(&choice["message"])?;
            (
                choice["message"]["content"].as_str().map(str::to_owned),
                choice["finish_reason"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_owned(),
            )
        }
        "openai_responses" => {
            if response["status"] != "completed" {
                return Err(error(
                    "ER_AI_INCOMPLETE",
                    "provider response is not completed",
                ));
            }
            let mut text = String::new();
            let mut found = false;
            for item in response["output"]
                .as_array()
                .ok_or_else(|| error("ER_AI_INVALID_RESPONSE", "missing output items"))?
            {
                match item["type"].as_str() {
                    Some("reasoning") => {}
                    Some("message") => {
                        if item["role"] != "assistant" || item["status"] != "completed" {
                            return Err(error(
                                "ER_AI_INVALID_RESPONSE",
                                "invalid output message status or role",
                            ));
                        }
                        for content in item["content"].as_array().ok_or_else(|| {
                            error("ER_AI_INVALID_RESPONSE", "missing message content")
                        })? {
                            match content["type"].as_str() {
                                Some("output_text") => {
                                    found = true;
                                    text.push_str(content["text"].as_str().ok_or_else(|| {
                                        error("ER_AI_INVALID_RESPONSE", "missing output text")
                                    })?);
                                }
                                Some("refusal") => {
                                    return Err(error("ER_AI_REFUSED", "provider refused"))
                                }
                                _ => {
                                    return Err(error(
                                        "ER_AI_INVALID_RESPONSE",
                                        "unsupported output content type",
                                    ))
                                }
                            }
                        }
                    }
                    _ => {
                        return Err(error(
                            "ER_AI_INVALID_RESPONSE",
                            "unexpected output item; tools are disabled",
                        ))
                    }
                }
            }
            (found.then_some(text), "stop".into())
        }
        _ => {
            return Err(error(
                "ER_AI_WRONG_MODEL_KIND",
                "connection does not support text",
            ))
        }
    };
    if finish != "stop" {
        return Err(error(
            if finish == "content_filter" {
                "ER_AI_REFUSED"
            } else {
                "ER_AI_INCOMPLETE"
            },
            format!("generation ended with {finish}"),
        ));
    }
    let text = text.ok_or_else(|| error("ER_AI_INVALID_RESPONSE", "missing generated text"))?;
    let output_json = if options["response_format"]["type"] == "json_schema" {
        let parsed = parse_json(&text, 8 * 1024 * 1024)
            .map_err(|_| error("ER_AI_OUTPUT_SCHEMA", "generated output is not valid JSON"))?;
        if !jsonschema::is_valid(&options["response_format"]["schema"], &parsed) {
            return Err(error(
                "ER_AI_OUTPUT_SCHEMA",
                "generated output does not match schema",
            ));
        }
        parsed
    } else {
        Json::Null
    };
    if !detail {
        return Ok((Some(Value::String(text)), Json::Null));
    }
    Ok((
        None,
        json!({"text":text,"output_json":output_json,"finish_reason":finish,"provider_finish_reason":finish}),
    ))
}
fn usage(response: &Json, connection: &Connection, embedding: bool) -> Result<Json, QueryError> {
    fn token(value: Option<&Json>) -> Result<Option<u64>, QueryError> {
        match value {
            None | Some(Json::Null) => Ok(None),
            Some(value) => value
                .as_u64()
                .map(Some)
                .ok_or_else(|| error("ER_AI_INVALID_RESPONSE", "invalid token usage")),
        }
    }
    if connection.protocol == "ollama" {
        let input = token(response.get("prompt_eval_count"))?;
        let output = if embedding {
            None
        } else {
            token(response.get("eval_count"))?
        };
        Ok(
            json!({"input_tokens":input,"output_tokens":output,"total_tokens":if embedding{input}else{input.zip(output).and_then(|(i,o)|i.checked_add(o))}}),
        )
    } else {
        let u = &response["usage"];
        if !u.is_null() && !u.is_object() {
            return Err(error("ER_AI_INVALID_RESPONSE", "invalid token usage"));
        }
        Ok(
            json!({"input_tokens":token(u.get("input_tokens").or_else(||u.get("prompt_tokens")))?,"output_tokens":token(u.get("output_tokens").or_else(||u.get("completion_tokens")))?,"total_tokens":token(u.get("total_tokens"))?}),
        )
    }
}
fn returnable(error: &QueryError) -> bool {
    use crate::error::catalog::*;
    error.is_any(&[
        ER_AI_UPSTREAM_AUTH,
        ER_AI_UPSTREAM_ERROR,
        ER_AI_INVALID_RESPONSE,
        ER_AI_DIMENSION_MISMATCH,
        ER_AI_INCOMPLETE,
        ER_AI_REFUSED,
        ER_AI_OUTPUT_SCHEMA,
        ER_AI_TIMEOUT,
    ])
}

pub(crate) fn call(name: &str, args: &[Value]) -> Result<Value, QueryError> {
    if !(2..=3).contains(&args.len()) {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "AI functions expect 2 or 3 arguments",
        ));
    }
    CALLS.with(|state| {
        let mut state = state.borrow_mut();
        let scope = state.as_mut().ok_or_else(|| {
            error(
                "ER_AI_UNSUPPORTED_CONTEXT",
                "AI functions require final projection or single SET",
            )
        })?;
        run_call(scope, name, args)
    })
}
enum PreparedOutcome {
    Immediate(Value),
    Call(PreparedCall),
}
struct PreparedCall {
    model: Model,
    connection: Connection,
    options: Json,
    local: LocalConfig,
    dispatch: Dispatch,
    url: String,
    body: Json,
    credential: Option<Zeroizing<Vec<u8>>>,
    deadline: Instant,
    response_limit: usize,
    memory: Arc<super::admission::Memory>,
    started: Instant,
    envelope: Json,
    embed: bool,
    detail: bool,
    count: usize,
    return_error: bool,
}
fn count_attempt(scope: &mut CallScope, dispatch: &Dispatch) -> Result<(), QueryError> {
    scope.attempts += 1;
    if scope.attempts
        > dispatch
            .user
            .as_ref()
            .and_then(|u| u.max_http_attempts_per_statement)
            .unwrap_or(100)
            .min(100)
    {
        return Err(error(
            "ER_AI_BUDGET_EXCEEDED",
            "HTTP attempt budget exceeded",
        ));
    }
    Ok(())
}
fn incomplete_finish_reason(response: &Json, connection: &Connection) -> &'static str {
    let length = match connection.protocol.as_str() {
        "ollama" => response["done_reason"] == "length",
        "openai_chat" => response["choices"][0]["finish_reason"] == "length",
        "openai_responses" => {
            response["status"] == "incomplete"
                && response["incomplete_details"]["reason"] == "max_output_tokens"
        }
        _ => false,
    };
    if length {
        "length"
    } else {
        "incomplete"
    }
}

fn finish_reply(
    call: &mut PreparedCall,
    reply: ProviderReply,
    attempts: u64,
) -> Result<Value, QueryError> {
    let response = &reply.body;
    let (simple, fields) = decode_output(
        response,
        &call.connection,
        &call.model,
        call.count,
        &call.options,
        call.detail,
    )
    .map_err(|e| {
        if e.is(crate::error::catalog::ER_AI_INCOMPLETE) {
            call.envelope["finish_reason"] =
                json!(incomplete_finish_reason(response, &call.connection));
        }
        e
    })?;
    let usage = usage(response, &call.connection, call.embed)?;
    if response
        .get("model")
        .is_some_and(|model| !model.is_null() && model.as_str().is_none_or(|s| s.len() > 1024))
    {
        return Err(error("ER_AI_INVALID_RESPONSE", "invalid provider model"));
    }
    if let Some(value) = simple {
        return Ok(value);
    }
    call.envelope["provider_model"] = response.get("model").cloned().unwrap_or(Json::Null);
    call.envelope["provider_request_id"] = json!(reply.request_id);
    call.envelope["attempts"] = json!(attempts);
    call.envelope["timing_ms"]["queue"] = json!(reply.queue.as_millis());
    call.envelope["timing_ms"]["upstream"] = json!(reply.upstream.as_millis());
    call.envelope["timing_ms"]["total"] = json!(call.started.elapsed().as_millis());
    call.envelope["usage"] = usage;
    merge(&mut call.envelope, fields);
    Ok(Value::Json(crate::engine::json::from_serde(&call.envelope)))
}
fn finish_call(
    mut call: PreparedCall,
    result: Result<Value, QueryError>,
    attempts: u64,
    failure: Option<&ProviderFailure>,
) -> Result<Value, QueryError> {
    TOTAL_CALLS.fetch_add(1, Ordering::Relaxed);
    if result.is_err() {
        FAILED_CALLS.fetch_add(1, Ordering::Relaxed);
    }
    if crate::engine::audit_log::is_active() {
        crate::engine::audit_log::record_ai_call(
            &json!({"event":"ai_call","model_id":call.model.id,"model_revision":call.model.model_revision,"principal":call.dispatch.principal,"request_id":call.envelope["request_id"],"attempts":attempts,"ok":result.is_ok(),"error_code":result.as_ref().err().map(|e|e.code()),"total_ms":call.started.elapsed().as_millis()}),
        );
    }
    match result {
        Err(e) if call.return_error && returnable(&e) => {
            call.envelope["ok"] = json!(false);
            call.envelope["attempts"] = json!(attempts);
            call.envelope["error"] = json!({"code":e.symbol(),"message":e.message(),"retryable":failure.is_some_and(|f|f.retryable),"ambiguous":failure.is_some_and(|f|f.ambiguous),"http_status":failure.and_then(|f|f.status)});
            call.envelope["finish_reason"] = if e.is(crate::error::catalog::ER_AI_INCOMPLETE) {
                call.envelope["finish_reason"].clone()
            } else if e.is(crate::error::catalog::ER_AI_REFUSED) {
                json!("refusal")
            } else {
                Json::Null
            };
            call.envelope["timing_ms"]["total"] = json!(call.started.elapsed().as_millis());
            Ok(Value::Json(crate::engine::json::from_serde(&call.envelope)))
        }
        Err(e) if e.symbol().is_some_and(|name| name.starts_with("ER_AI_")) => {
            Err(e.spec().unwrap().error(format!(
                "{}; model_id={}; request_id={}",
                e.message(),
                call.model.id,
                call.envelope["request_id"].as_str().unwrap_or_default()
            )))
        }
        other => other,
    }
}
fn execute_prepared(scope: &mut CallScope, mut call: PreparedCall) -> Result<Value, QueryError> {
    let max_retries = call
        .options
        .get("max_retries")
        .and_then(Json::as_u64)
        .unwrap_or(0);
    let retry_ambiguous = call
        .options
        .get("retry_ambiguous")
        .and_then(Json::as_bool)
        .unwrap_or(false);
    let mut attempts = 0u64;
    let mut last_failure = None;
    let result = (|| loop {
        count_attempt(scope, &call.dispatch)?;
        attempts += 1;
        match send(&call) {
            Ok(reply) => return finish_reply(&mut call, reply, attempts),
            Err(failure) => {
                let retry = attempts <= max_retries
                    && failure.retryable
                    && (!failure.ambiguous || retry_ambiguous);
                let retry_after = failure.retry_after;
                let err = failure.error.clone();
                last_failure = Some(failure);
                if !retry {
                    return Err(err);
                }
                use rand::Rng;
                let jitter = Duration::from_millis(
                    rand::thread_rng()
                        .gen_range(0..=(100u64.saturating_mul(1u64 << attempts.min(6))).min(5000)),
                );
                let delay = retry_after.unwrap_or_default().max(jitter);
                let end = Instant::now()
                    .checked_add(delay)
                    .filter(|t| *t < call.deadline)
                    .ok_or_else(|| {
                        error("ER_AI_TIMEOUT", "retry delay exceeds remaining deadline")
                    })?;
                while Instant::now() < end {
                    crate::engine::cancel::check_cancelled()?;
                    call.dispatch.check()?;
                    std::thread::sleep(
                        end.saturating_duration_since(Instant::now())
                            .min(Duration::from_millis(10)),
                    );
                }
            }
        }
    })();
    scope.outputs.push(call.memory.clone());
    finish_call(call, result, attempts, last_failure.as_ref())
}
fn run_call(scope: &mut CallScope, name: &str, args: &[Value]) -> Result<Value, QueryError> {
    match prepare_call(scope, name, args)? {
        PreparedOutcome::Immediate(value) => Ok(value),
        PreparedOutcome::Call(call) => execute_prepared(scope, call),
    }
}
fn prepare_call(
    scope: &mut CallScope,
    name: &str,
    args: &[Value],
) -> Result<PreparedOutcome, QueryError> {
    let started = Instant::now();
    if scope.control.cancelled.load(Ordering::Acquire) {
        return Err(error("ER_AI_DISABLED", "AI execution is shutting down"));
    }
    if !enabled() {
        return Err(error("ER_AI_DISABLED", "AI calls are disabled"));
    }
    if matches!(args[0], Value::Null) {
        return Ok(PreparedOutcome::Immediate(Value::Null));
    }
    let embed =
        name.eq_ignore_ascii_case("AI_EMBED") || name.eq_ignore_ascii_case("AI_EMBED_DETAIL");
    let detail = name.to_ascii_uppercase().ends_with("_DETAIL");
    let raw = text(&args[0])?;
    let model_name = if raw.contains('.') {
        raw.to_ascii_lowercase()
    } else {
        format!("{}.{}", scope.database, raw.to_ascii_lowercase())
    };
    let state = scope.catalog.clone();
    let model = state
        .models
        .get(&model_name)
        .ok_or_else(|| error("ER_AI_MODEL_NOT_FOUND", "model does not exist"))?
        .clone();
    if authorize(&scope.db, &scope.auth, "AI_USE").is_err() {
        authorize(
            &scope.db,
            &scope.auth,
            &format!("AI_MODEL_{}_USE", model.id),
        )?;
    }
    if (model.kind == "embedding") != embed {
        return Err(error("ER_AI_WRONG_MODEL_KIND", "wrong model kind"));
    }
    if !model.enabled {
        return Err(error("ER_AI_DISABLED", "model is disabled"));
    }
    if matches!(args[1], Value::Null) {
        return Ok(PreparedOutcome::Immediate(Value::Null));
    }
    let connection = state
        .connections
        .get(&model.connection)
        .filter(|c| c.enabled)
        .ok_or_else(|| error("ER_AI_DISABLED", "connection is unavailable"))?
        .clone();
    let mut options = Json::Object(model.defaults.clone());
    let supplied = args
        .get(2)
        .map(as_json)
        .transpose()?
        .unwrap_or_else(|| json!({}));
    if !supplied.is_object() {
        return Err(error("ER_AI_INVALID_ARGUMENT", "options must be an object"));
    }
    for field in ["timeout_ms", "max_retries", "retry_ambiguous", "on_error"] {
        if supplied.get(field).is_some_and(Json::is_null) {
            return Err(error(
                "ER_AI_INVALID_ARGUMENT",
                "execution controls cannot be null",
            ));
        }
    }
    merge_options(&mut options, supplied);
    validate_options(&model, &options)?;
    let return_error = options["on_error"] == "return";
    if return_error && !detail {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "on_error=return requires a DETAIL function",
        ));
    }
    let (input, count) = input_json(&args[1], embed, detail, &model)?;
    let input_bytes = if let Some(s) = input.as_str() {
        s.len()
    } else {
        input
            .as_array()
            .unwrap()
            .iter()
            .map(|v| {
                if embed {
                    v.as_str().unwrap().len()
                } else {
                    v["content"].as_str().unwrap().len()
                }
            })
            .sum()
    };
    if !embed && input_bytes > model.limits.max_input_bytes.unwrap_or(262144).min(262144) {
        return Err(error("ER_AI_INVALID_ARGUMENT", "input exceeds byte limit"));
    }
    let local = LocalConfig::load()?;
    let user = local
        .user_limits
        .iter()
        .find(|u| {
            u.user == scope.auth.authenticated_as.user && u.host == scope.auth.authenticated_as.host
        })
        .cloned();
    scope.items += count;
    if scope.items
        > user
            .as_ref()
            .and_then(|u| u.max_call_items_per_statement)
            .unwrap_or(100)
            .min(100)
        || scope.attempts
            > user
                .as_ref()
                .and_then(|u| u.max_http_attempts_per_statement)
                .unwrap_or(100)
                .min(100)
    {
        return Err(error(
            "ER_AI_BUDGET_EXCEEDED",
            "statement AI budget exceeded",
        ));
    }
    let credential_state = scope.db.ai_catalog()?;
    let current_secret = if let Some(name) = connection.auth.secret() {
        let expected = state
            .secrets
            .get(name)
            .ok_or_else(|| error("ER_AI_CREDENTIAL_UNAVAILABLE", "pinned secret is missing"))?;
        let live = credential_state
            .secrets
            .get(name)
            .filter(|s| s.id == expected.id)
            .ok_or_else(|| error("ER_AI_CREDENTIAL_UNAVAILABLE", "secret identity changed"))?;
        Some((name.to_owned(), live.clone()))
    } else {
        None
    };
    let dispatch = Dispatch {
        db: scope.db.clone(),
        auth: scope.auth.clone(),
        model_name: model_name.clone(),
        model: model.clone(),
        connection_id: connection.id,
        connection_generation: connection.generation,
        namespace: scope.control.namespace.clone(),
        principal: format!(
            "{}@{}",
            scope.auth.authenticated_as.user, scope.auth.authenticated_as.host
        ),
        user,
        secret: current_secret
            .as_ref()
            .map(|(name, s)| (name.clone(), s.id, s.generation)),
        control: scope.control.clone(),
        local: local.clone(),
        breaker_key: format!("{}:{}", scope.control.namespace, connection.id),
        breaker_revision: (
            connection.generation,
            local.generation,
            current_secret
                .as_ref()
                .map(|(_, secret)| secret.generation)
                .unwrap_or(0),
        ),
    };
    let credential = if let Some((_, secret)) = &current_secret {
        let keys = load_keys(scope.db.data_dir(), &credential_state, false)?;
        Some(decrypt_secret(&credential_state, &keys, secret)?)
    } else {
        None
    };
    let (url, body) = request_body(&model, &connection, &input, &options)?;
    let request_bytes = serde_json::to_vec(&body)
        .map_err(|_| error("ER_AI_INVALID_ARGUMENT", "cannot encode request"))?
        .len();
    if request_bytes > 8 * 1024 * 1024 {
        return Err(error("ER_AI_BUDGET_EXCEEDED", "request exceeds 8 MiB"));
    }
    let response_limit = if embed {
        count
            .saturating_mul(model.embedding.as_ref().unwrap().dimensions)
            .saturating_mul(32)
            .saturating_add(65536)
    } else {
        positive(
            &options,
            "max_output_tokens",
            1024,
            model.limits.max_output_tokens.unwrap_or(4096).min(4096),
        )? as usize
            * 128
            + 65536
    }
    .min(8 * 1024 * 1024);
    // Bound the receive buffer and overlapping old/new allocations during growth.
    // DOM and result storage are charged from the actual JSON before parsing.
    let reserved = request_bytes
        .saturating_mul(4)
        .saturating_add(response_limit.saturating_mul(3));
    let memory = super::admission::Memory::reserve(reserved)?;
    let timeout = positive(&options, "timeout_ms", 30000, 60000)?;
    let deadline = scope.deadline.min(started + Duration::from_millis(timeout));
    use rand::RngCore;
    let mut id = [0; 16];
    rand::rngs::OsRng.fill_bytes(&mut id);
    let request_id = id.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let envelope = json!({"ok":true,"model":model_name,"model_revision":model.model_revision,"provider_model":null,"request_id":request_id,"provider_request_id":null,"attempts":1,"cache_hit":false,"timing_ms":{"queue":null,"upstream":null,"total":null},"text":null,"output_json":null,"data":null,"finish_reason":null,"usage":{"input_tokens":null,"output_tokens":null,"total_tokens":null},"error":null});
    Ok(PreparedOutcome::Call(PreparedCall {
        model,
        connection,
        options,
        local,
        dispatch,
        url,
        body,
        credential,
        deadline,
        response_limit,
        memory,
        started,
        envelope,
        embed,
        detail,
        count,
        return_error,
    }))
}
/// Network waits never consume the ordinary SQL worker pool.
pub(crate) fn spawn_coordinator<F, T>(work: F) -> crate::util::blocking_pool::BlockingHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    static POOL: OnceLock<crate::util::blocking_pool::BlockingPool> = OnceLock::new();
    POOL.get_or_init(|| crate::util::blocking_pool::BlockingPool::with_limits(2, 8))
        .spawn(work)
}
pub(crate) fn admit_coordinator() -> Result<tokio::sync::OwnedSemaphorePermit, QueryError> {
    validate_runtime_limits()?;
    static ADMISSION: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    ADMISSION
        .get_or_init(|| {
            Arc::new(tokio::sync::Semaphore::new(
                env_limit("MYDB_AI_MAX_QUEUE", 128) + 8,
            ))
        })
        .clone()
        .try_acquire_owned()
        .map_err(|_| error("ER_AI_QUEUE_FULL", "external SQL coordinator queue is full"))
}

pub(crate) fn status() -> Vec<(&'static str, u64)> {
    vec![
        ("enabled", u64::from(enabled())),
        ("completed_http_calls", TOTAL_CALLS.load(Ordering::Relaxed)),
        ("failed_http_calls", FAILED_CALLS.load(Ordering::Relaxed)),
        ("http_attempts", HTTP_ATTEMPTS.load(Ordering::Relaxed)),
        ("queued_and_running", PENDING.load(Ordering::Acquire) as u64),
        ("memory_bytes", super::admission::memory_used() as u64),
    ]
}

pub(crate) fn validate_defaults(model: &Model) -> Result<(), QueryError> {
    validate_options(model, &Json::Object(model.defaults.clone()))
}

/// Batch only direct embedding occurrences with identical call parameters.
/// Conditional expressions retain the ordinary evaluator and its branch masks.
pub(crate) fn embed_batch(
    model: Value,
    texts: Vec<String>,
    options: Value,
) -> Result<Vec<Value>, QueryError> {
    let options_json = as_json(&options)?;
    if options_json.get("on_error").is_some_and(|v| v == "return") {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "AI_EMBED does not support on_error=return",
        ));
    }
    let input = Value::Json(crate::engine::json::from_serde(&json!(texts)));
    let detail = call("AI_EMBED_DETAIL", &[model, input, options])?;
    let Value::Json(bytes) = detail else {
        return Err(error(
            "ER_AI_INVALID_RESPONSE",
            "batch embedding did not return JSON",
        ));
    };
    let parsed = crate::engine::json::to_serde(&bytes)?;
    let data = parsed["data"]
        .as_array()
        .ok_or_else(|| error("ER_AI_INVALID_RESPONSE", "missing batch embeddings"))?;
    let mut out = Vec::with_capacity(data.len());
    for item in data {
        let array = item["embedding"]
            .as_array()
            .ok_or_else(|| error("ER_AI_INVALID_RESPONSE", "invalid batch embedding"))?;
        let mut bytes = Vec::with_capacity(array.len() * 4);
        for n in array {
            let n = n
                .as_f64()
                .filter(|n| n.is_finite())
                .ok_or_else(|| error("ER_AI_INVALID_RESPONSE", "invalid FLOAT32 embedding"))?
                as f32;
            bytes.extend_from_slice(&n.to_le_bytes());
        }
        out.push(Value::Vector(bytes.into()));
    }
    Ok(out)
}

pub(crate) fn last_statement_stats() -> (usize, usize) {
    LAST.with(std::cell::Cell::get)
}

#[cfg(test)]
mod codec_tests {
    use super::*;
    fn connection(protocol: &str) -> Connection {
        serde_json::from_value(json!({"protocol":protocol,"base_url":"https://models.invalid/v1","auth":{"type":"none"}})).unwrap()
    }
    fn model(embedding: bool) -> Model {
        serde_json::from_value(if embedding{json!({"kind":"embedding","remote_model":"embed","model_revision":"v1","embedding":{"dimensions":3,"request_dimensions":true}})}else{json!({"kind":"text_generation","remote_model":"chat","model_revision":"v1","capabilities":{"temperature":true,"json_schema":true,"messages":true},"limits":{"max_output_tokens":256}})}).unwrap()
    }
    #[test]
    fn ai_adapters_map_budget_fields_without_overwriting_base_path() {
        let m = model(false);
        let options = json!({"max_output_tokens":32,"temperature":0.2});
        for (protocol, path, field) in [
            (
                "openai_chat",
                "/v1/chat/completions",
                "max_completion_tokens",
            ),
            ("openai_responses", "/v1/responses", "max_output_tokens"),
            ("ollama", "/v1/api/chat", "num_predict"),
        ] {
            let (url, body) =
                request_body(&m, &connection(protocol), &json!("hello"), &options).unwrap();
            assert!(url.ends_with(path));
            assert_eq!(body["stream"], false);
            assert_eq!(
                if protocol == "ollama" {
                    &body["options"][field]
                } else {
                    &body[field]
                },
                32
            );
            if protocol != "ollama" {
                assert_eq!(body["store"], false);
            }
        }
        let mut legacy = connection("openai_chat");
        legacy.profile = Some("openai_chat_legacy".into());
        let (_, body) = request_body(&m, &legacy, &json!("hello"), &options).unwrap();
        assert_eq!(body["max_tokens"], 32);
        assert!(body.get("max_completion_tokens").is_none());
        let (_, body) = request_body(
            &model(true),
            &connection("ollama"),
            &json!(["one", "two"]),
            &json!({}),
        )
        .unwrap();
        assert_eq!(body["truncate"], false);
        assert_eq!(body["dimensions"], 3);
    }
    #[test]
    fn ai_batch_input_enforces_per_item_limits_and_binary_json_depth() {
        let mut m = model(true);
        m.limits.max_input_bytes = Some(8);
        let input = Value::Json(crate::engine::json::from_serde(&json!([
            "12345678", "abcdefgh"
        ])));
        assert_eq!(input_json(&input, true, true, &m).unwrap().1, 2);
        let too_long = Value::Json(crate::engine::json::from_serde(&json!(["123456789"])));
        assert!(input_json(&too_long, true, true, &m).is_err());
        let mut nested = json!(0);
        for _ in 0..33 {
            nested = json!([nested]);
        }
        assert!(as_json(&Value::Json(crate::engine::json::from_serde(&nested))).is_err());
        assert!(usage(
            &json!({"usage":{"total_tokens":"12"}}),
            &connection("openai_chat"),
            false
        )
        .is_err());
        assert!(usage(
            &json!({"prompt_eval_count":-1}),
            &connection("ollama"),
            true
        )
        .is_err());
    }
    #[test]
    fn ai_embedding_indices_dimensions_and_float32_validation() {
        let m = model(true);
        let c = connection("openai_embeddings");
        let valid =
            json!({"data":[{"index":1,"embedding":[1,2,3]},{"index":0,"embedding":[4,5,6]}]});
        let out = vectors(&valid, &c, &m, 2).unwrap();
        assert_eq!(out, vec![vec![4., 5., 6.], vec![1., 2., 3.]]);
        for bad in [
            json!({"data":[{"index":0,"embedding":[1,2,3]},{"index":0,"embedding":[1,2,3]}]}),
            json!({"data":[{"index":0,"embedding":[1,2]}]}),
            json!({"data":[{"index":0,"embedding":[1e39,2,3]}]}),
            json!({"data":[{"index":0,"embedding":["1",2,3]}]}),
        ] {
            assert!(vectors(
                &bad,
                &c,
                &m,
                if bad["data"].as_array().unwrap().len() == 2 {
                    2
                } else {
                    1
                }
            )
            .is_err());
        }
        let mut normalized = m;
        normalized.embedding.as_mut().unwrap().normalize = "l2".into();
        assert!(vectors(
            &json!({"data":[{"index":0,"embedding":[0,0,0]}]}),
            &c,
            &normalized,
            1
        )
        .is_err());
    }
    #[test]
    fn ai_response_decoding_never_confuses_reasoning_or_refusal_with_text() {
        let m = model(false);
        let c = connection("openai_responses");
        let good = json!({"status":"completed","output":[{"type":"reasoning"},{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"answer"}]}]});
        assert_eq!(
            decode_output(&good, &c, &m, 1, &json!({}), false)
                .unwrap()
                .0,
            Some(Value::String("answer".into()))
        );
        assert!(decode_output(
            &json!({"status":"completed","output":[{"type":"reasoning"}]}),
            &c,
            &m,
            1,
            &json!({}),
            false
        )
        .is_err());
        let refusal =
            json!({"choices":[{"message":{"refusal":"no","content":null},"finish_reason":"stop"}]});
        assert!(decode_output(
            &refusal,
            &connection("openai_chat"),
            &m,
            1,
            &json!({}),
            false
        )
        .unwrap_err()
        .message()
        .contains("REFUSED"));
        let length = json!({"choices":[{"message":{"role":"assistant","content":"partial"},"finish_reason":"length"}]});
        assert!(decode_output(
            &length,
            &connection("openai_chat"),
            &m,
            1,
            &json!({}),
            false
        )
        .unwrap_err()
        .message()
        .contains("INCOMPLETE"));
    }
    #[test]
    fn ai_private_parameters_cannot_override_owned_fields_or_budgets() {
        let mut m = model(false);
        m.provider_options_allowed = vec!["options.num_predict".into()];
        m.provider_options_schema
            .insert("options.num_predict".into(), json!({"type":"integer"}));
        assert!(validate_options(
            &m,
            &json!({"provider_options":{"options":{"num_predict":1000000}}})
        )
        .is_err());
        assert!(validate_options(&m, &json!({"max_output_tokens":257})).is_err());
        assert!(validate_options(&m, &json!({"unknown":1})).is_err());
        let mut defaults = json!({"temperature":0.5,"provider_options":{"x":1}});
        merge_options(
            &mut defaults,
            json!({"temperature":null,"provider_options":{"x":null}}),
        );
        assert!(defaults.get("temperature").is_none());
        assert!(defaults["provider_options"].get("x").unwrap().is_null());
    }
    #[test]
    fn ai_output_messages_reject_non_assistant_roles_and_tool_calls() {
        let m = model(false);
        let mut accepted = Vec::new();
        for protocol in ["ollama", "openai_chat"] {
            for message in [
                json!({"role":"user","content":"untrusted echo"}),
                json!({"role":"tool","content":"tool result"}),
                json!({"role":"assistant","content":"misleading text","tool_calls":[{"id":"call-1","type":"function","function":{"name":"run","arguments":"{}"}}]}),
                json!({"role":"assistant","content":"misleading text","function_call":{"name":"run","arguments":"{}"}}),
            ] {
                let response = if protocol == "ollama" {
                    json!({"done":true,"done_reason":"stop","message":message})
                } else {
                    json!({"choices":[{"index":0,"message":message,"finish_reason":"stop"}]})
                };
                let result =
                    decode_output(&response, &connection(protocol), &m, 1, &json!({}), false);
                match result {
                    Ok(_) => accepted.push(format!("{protocol}: {response}")),
                    Err(error) => assert!(
                        error.is(crate::error::catalog::ER_AI_INVALID_RESPONSE),
                        "{error:?}"
                    ),
                }
            }
        }
        assert!(
            accepted.is_empty(),
            "accepted invalid output messages: {accepted:#?}"
        );
    }
    #[test]
    fn ai_structured_output_validates_json_and_required_fields_locally() {
        let options = json!({"response_format":{"type":"json_schema","name":"test","strict":true,"schema":{"type":"object","properties":{"count":{"type":"integer"}},"required":["count"],"additionalProperties":false}}});
        let c = connection("ollama");
        let m = model(false);
        for text in [
            r#"{"count":"wrong"}"#,
            r#"{"count":1,"extra":true}"#,
            r#"{}"#,
            r#"{"count":1,"count":2}"#,
            "```json\n{\"count\":1}\n```",
            "not json",
        ] {
            let response = json!({"done":true,"done_reason":"stop","message":{"role":"assistant","content":text}});
            let err = decode_output(&response, &c, &m, 1, &options, true).unwrap_err();
            assert!(
                err.is(crate::error::catalog::ER_AI_OUTPUT_SCHEMA),
                "{text}: {err:?}"
            );
        }
        let response = json!({"done":true,"done_reason":"stop","message":{"role":"assistant","content":"{\"count\":2}"}});
        let (_, detail) = decode_output(&response, &c, &m, 1, &options, true).unwrap();
        assert_eq!(detail["output_json"], json!({"count":2}));
    }
    #[test]
    fn ai_responses_reject_unsupported_items_incomplete_messages_and_refusals() {
        let c = connection("openai_responses");
        let m = model(false);
        for item in [
            json!({"type":"function_call","name":"danger","arguments":"{}"}),
            json!({"type":"message","role":"user","status":"completed","content":[{"type":"output_text","text":"echo"}]}),
            json!({"type":"message","role":"assistant","status":"in_progress","content":[{"type":"output_text","text":"partial"}]}),
            json!({"type":"message","role":"assistant","status":"completed","content":[{"type":"image","url":"ignored"}]}),
        ] {
            let response = json!({"status":"completed","output":[item]});
            assert!(decode_output(&response, &c, &m, 1, &json!({}), true)
                .unwrap_err()
                .is(crate::error::catalog::ER_AI_INVALID_RESPONSE));
        }
        let response = json!({"status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"refusal","refusal":"no"}]}]});
        assert!(decode_output(&response, &c, &m, 1, &json!({}), true)
            .unwrap_err()
            .is(crate::error::catalog::ER_AI_REFUSED));
        for status in ["in_progress", "incomplete", "failed", "cancelled"] {
            assert!(decode_output(
                &json!({"status":status,"output":[]}),
                &c,
                &m,
                1,
                &json!({}),
                true
            )
            .unwrap_err()
            .is(crate::error::catalog::ER_AI_INCOMPLETE));
        }
    }
    #[test]
    fn ai_usage_distinguishes_missing_zero_and_malformed_token_counts() {
        let c = connection("openai_chat");
        assert_eq!(
            usage(&json!({}), &c, false).unwrap(),
            json!({"input_tokens":null,"output_tokens":null,"total_tokens":null})
        );
        assert_eq!(
            usage(
                &json!({"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}),
                &c,
                false
            )
            .unwrap(),
            json!({"input_tokens":0,"output_tokens":0,"total_tokens":0})
        );
        for bad in [
            json!(-1),
            json!(0.5),
            json!("12"),
            json!(false),
            json!([]),
            json!({}),
        ] {
            assert!(usage(&json!({"usage":{"total_tokens":bad}}), &c, false)
                .unwrap_err()
                .is(crate::error::catalog::ER_AI_INVALID_RESPONSE));
        }
        for bad in [json!([]), json!("bad"), json!(12)] {
            assert!(usage(&json!({"usage":bad}), &c, false).is_err());
        }
        let normalized = usage(
            &json!({"prompt_eval_count":4,"eval_count":3}),
            &connection("ollama"),
            false,
        )
        .unwrap();
        assert_eq!(normalized["total_tokens"], 7);
        assert_eq!(
            usage(&json!({"prompt_eval_count":4}), &connection("ollama"), true).unwrap()
                ["output_tokens"],
            Json::Null
        );
    }
    #[test]
    fn ai_embedding_l2_normalization_handles_extreme_finite_float32_values() {
        let mut m = model(true);
        m.embedding.as_mut().unwrap().normalize = "l2".into();
        for values in [
            json!([3.0, 4.0, 0.0]),
            json!([1e38, -1e38, 1e38]),
            json!([1e-38, -1e-38, 1e-38]),
        ] {
            let response = json!({"data":[{"index":0,"embedding":values}]});
            let result = vectors(&response, &connection("openai_embeddings"), &m, 1).unwrap();
            let norm = result[0]
                .iter()
                .map(|v| (*v as f64).powi(2))
                .sum::<f64>()
                .sqrt();
            assert!((norm - 1.0).abs() < 1e-6, "{result:?}");
            assert!(result[0].iter().all(|v| v.is_finite()));
        }
        for index in [json!(-1), json!(0.5), json!("0"), json!(1)] {
            let response = json!({"data":[{"index":index,"embedding":[1,2,3]}]});
            assert!(vectors(&response, &connection("openai_embeddings"), &m, 1).is_err());
        }
    }
    #[test]
    fn ai_input_json_preserves_text_and_enforces_utf8_byte_boundaries() {
        let mut m = model(false);
        m.limits.max_input_bytes = Some(8);
        assert_eq!(
            input_json(&Value::String("😀😀".into()), false, false, &m)
                .unwrap()
                .0,
            json!("😀😀")
        );
        assert!(input_json(&Value::String("😀😀😀".into()), false, false, &m).is_err());
        assert_eq!(
            input_json(&Value::String("[1,2]".into()), false, false, &m)
                .unwrap()
                .0,
            json!("[1,2]")
        );
        for input in [
            Value::Int(1),
            Value::Bytes(vec![0, 255]),
            Value::Json(crate::engine::json::from_serde(&Json::Null)),
        ] {
            assert!(input_json(&input, false, false, &m).is_err());
        }
        for message in [
            json!([{"role":"tool","content":"text"}]),
            json!([{"role":"system","content":"text"}]),
            json!([{"role":"user","content":[{"text":"not supported"}]}]),
            json!([{"role":"user","content":"x","unknown":true}]),
        ] {
            assert!(input_json(
                &Value::Json(crate::engine::json::from_serde(&message)),
                false,
                false,
                &m
            )
            .is_err());
        }
    }
    #[test]
    fn ai_sampling_controls_and_provider_paths_obey_caps_and_reservations() {
        let mut m = model(false);
        m.capabilities.extend([
            ("top_p".into(), true),
            ("stop".into(), true),
            ("seed".into(), true),
        ]);
        assert!(validate_options(&m,&json!({"temperature":0,"top_p":1,"stop":["end"],"seed":-42,"max_output_tokens":256,"max_retries":0})).is_ok());
        for options in [
            json!({"temperature":-0.1}),
            json!({"temperature":2.1}),
            json!({"top_p":0}),
            json!({"top_p":1.1}),
            json!({"stop":[]}),
            json!({"stop":[""]}),
            json!({"stop":["1","2","3","4","5"]}),
            json!({"seed":1.5}),
            json!({"max_output_tokens":257}),
        ] {
            assert!(
                validate_options(&m, &options)
                    .unwrap_err()
                    .is(crate::error::catalog::ER_AI_INVALID_ARGUMENT),
                "{options}"
            );
        }
        for path in [
            "model",
            "stream",
            "headers.authorization",
            "tools",
            "options.num_predict",
        ] {
            m.provider_options_allowed = vec![path.into()];
            m.provider_options_schema
                .insert(path.into(), json!({"type":"integer"}));
            let mut nested = json!(1);
            for key in path.split('.').rev() {
                nested = json!({key:nested});
            }
            assert!(
                validate_options(&m, &json!({"provider_options":nested})).is_err(),
                "accepted reserved {path}"
            );
        }
    }
    #[test]
    fn ai_text_outputs_allow_empty_tool_metadata_without_exposing_reasoning() {
        let message = json!({"role":"assistant","content":"answer","thinking":"hidden reasoning","refusal":null,"tool_calls":[],"function_call":null});
        for protocol in ["ollama", "openai_chat"] {
            let response = if protocol == "ollama" {
                json!({"done":true,"done_reason":"stop","message":message})
            } else {
                json!({"choices":[{"message":message,"finish_reason":"stop"}]})
            };
            let (simple, _) = decode_output(
                &response,
                &connection(protocol),
                &model(false),
                1,
                &json!({}),
                false,
            )
            .unwrap();
            assert_eq!(simple, Some(Value::String("answer".into())));
            let (_, detail) = decode_output(
                &response,
                &connection(protocol),
                &model(false),
                1,
                &json!({}),
                true,
            )
            .unwrap();
            assert_eq!(detail["text"], "answer");
            assert!(!detail.to_string().contains("hidden reasoning"));
        }
    }
    #[test]
    fn ai_metadata_only_lifetime_shutdown_does_not_start_http_runtime() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::Builder::new()
            .prefix("ai-lifetime-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let before = RUNTIME.get().is_some();
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                crate::storage::StorageConfig {
                    durability: crate::storage::DurabilityMode::Memory,
                    checkpoint_interval: Duration::from_secs(60),
                    checkpoint_wal_bytes: 64 * 1024 * 1024,
                    lock_wait: Duration::from_secs(1),
                    group_commit_window: Duration::from_micros(0),
                    wal_segment_bytes: 64 * 1024 * 1024,
                    buffer_pool_pages: 512,
                    buffer_pool_io_threads: 1,
                    buffer_pool_prefetch_pages: 0,
                    undo_retention: Duration::from_secs(60),
                    heap_update_reserve_bytes: 2048,
                    startup_validation: crate::storage::StartupValidationMode::Off,
                    startup_validate_indexes: false,
                },
            )
            .unwrap();
        let scope = prepare_calls(
            engine.database_for_replication().clone(),
            &SessionContext::default(),
        );
        drop(scope.unwrap());
        engine.shutdown().unwrap();
        assert_eq!(
            RUNTIME.get().is_some(),
            before,
            "shutdown initialized an unused HTTP runtime"
        );
    }
}

struct DatabaseControl {
    cancelled: std::sync::atomic::AtomicBool,
    active: AtomicUsize,
    pending: AtomicUsize,
    namespace: String,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}
type Controls = std::sync::Mutex<HashMap<usize, (std::sync::Weak<Database>, Arc<DatabaseControl>)>>;
static CONTROLS: OnceLock<Controls> = OnceLock::new();
fn database_control(db: &Arc<Database>) -> Result<Arc<DatabaseControl>, QueryError> {
    let mut controls = CONTROLS
        .get_or_init(|| std::sync::Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| error("ER_AI_DISABLED", "AI lifecycle state poisoned"))?;
    let key = Arc::as_ptr(db) as usize;
    if let Some((_, control)) = controls.get(&key) {
        return Ok(control.clone());
    }
    if controls.len() >= 256 {
        controls.retain(|_, (db, _)| db.strong_count() != 0);
        if controls.len() >= 256 {
            return Err(error(
                "ER_AI_BUDGET_EXCEEDED",
                "too many AI database lifetimes",
            ));
        }
    }
    let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
        1024,
    )?;
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let identity = NEXT
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |id| id.checked_add(1))
        .map_err(|_| {
            error(
                "ER_AI_BUDGET_EXCEEDED",
                "AI database lifetime identity space exhausted",
            )
        })?;
    let control = Arc::new(DatabaseControl {
        namespace: format!("db-{identity}"),
        pending: AtomicUsize::new(0),
        cancelled: std::sync::atomic::AtomicBool::new(false),
        active: AtomicUsize::new(0),
        _charge: charge,
    });
    controls.insert(key, (Arc::downgrade(db), control.clone()));
    Ok(control)
}
impl Drop for CallScope {
    fn drop(&mut self) {
        self.control.active.fetch_sub(1, Ordering::AcqRel);
    }
}
fn retire_runtime_namespace(namespace: &str) {
    super::admission::retire_namespace(namespace);
    let prefix = format!("{namespace}:");
    if let Some(Ok(runtime)) = RUNTIME.get() {
        if let Ok(mut clients) = runtime.clients.try_lock() {
            clients.retain(|key, _| !key.starts_with(&prefix));
        }
    }
}
pub(crate) fn shutdown_database(db: &Arc<Database>) -> Result<(), QueryError> {
    let Some(controls) = CONTROLS.get() else {
        return Ok(());
    };
    let control = controls
        .lock()
        .map_err(|_| error("ER_AI_DISABLED", "AI lifecycle state poisoned"))?
        .get(&(Arc::as_ptr(db) as usize))
        .map(|(_, control)| control.clone());
    let Some(control) = control else {
        return Ok(());
    };
    control.cancelled.store(true, Ordering::Release);
    let until = Instant::now() + Duration::from_secs(2);
    while (control.active.load(Ordering::Acquire) != 0
        || control.pending.load(Ordering::Acquire) != 0)
        && Instant::now() < until
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    if control.pending.load(Ordering::Acquire) == 0 {
        retire_runtime_namespace(&control.namespace);
    }
    Ok(())
}

pub(crate) fn call_many(name: &str, arguments: Vec<Vec<Value>>) -> Result<Vec<Value>, QueryError> {
    CALLS.with(|slot| {
        let mut scope = slot.borrow_mut();
        let scope = scope
            .as_mut()
            .ok_or_else(|| error("ER_AI_UNSUPPORTED_CONTEXT", "missing external scope"))?;
        call_many_in_scope(scope, name, arguments)
    })
}
fn call_many_in_scope(
    scope: &mut CallScope,
    name: &str,
    arguments: Vec<Vec<Value>>,
) -> Result<Vec<Value>, QueryError> {
    if arguments.iter().any(|args| !(2..=3).contains(&args.len())) {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "AI functions expect 2 or 3 arguments",
        ));
    }
    let calls = arguments
        .iter()
        .map(|args| prepare_call(scope, name, args))
        .collect::<Result<Vec<_>, _>>()?;
    if calls.iter().any(|outcome|matches!(outcome,PreparedOutcome::Call(call) if call.options.get("max_retries").and_then(Json::as_u64).unwrap_or(0)>0)) {
        return calls.into_iter().map(|outcome|match outcome{PreparedOutcome::Immediate(value)=>Ok(value),PreparedOutcome::Call(call)=>execute_prepared(scope,call)}).collect();
    }
    let (wake_tx, wake_rx) = std::sync::mpsc::channel();
    let mut values = vec![None; calls.len()];
    let mut pending = Vec::new();
    for (index, outcome) in calls.into_iter().enumerate() {
        match outcome {
            PreparedOutcome::Immediate(value) => values[index] = Some(value),
            PreparedOutcome::Call(call) => {
                count_attempt(scope, &call.dispatch)?;
                match start_pending(&call, Some(wake_tx.clone())) {
                    Ok(request) => pending.push((index, Some(call), Some(request))),
                    Err(failure) => {
                        let err = failure.error.clone();
                        scope.outputs.push(call.memory.clone());
                        values[index] = Some(finish_call(call, Err(err), 1, Some(&failure))?);
                    }
                }
            }
        }
    }
    while pending.iter().any(|(_, _, request)| request.is_some()) {
        for (index, call, request) in &mut pending {
            if let Some(result) = request.as_mut().and_then(PendingProvider::poll) {
                let mut call = call.take().unwrap();
                scope.outputs.push(call.memory.clone());
                let result = match result {
                    Ok(reply) => {
                        let decoded = finish_reply(&mut call, reply, 1);
                        finish_call(call, decoded, 1, None)
                    }
                    Err(failure) => {
                        let err = failure.error.clone();
                        finish_call(call, Err(err), 1, Some(&failure))
                    }
                };
                request.take();
                values[*index] = Some(result?);
            }
        }
        if pending.iter().any(|(_, _, request)| request.is_some()) {
            let _ = wake_rx.recv_timeout(Duration::from_millis(10));
        }
    }
    Ok(values.into_iter().map(Option::unwrap).collect())
}
