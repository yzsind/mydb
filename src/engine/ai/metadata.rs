//! Permission-filtered AI metadata contains no ciphertext or credential bytes.
use super::*;
use crate::{engine::auth::AuthorizationContext, storage::Database, types::Value};
fn can(db: &Database, auth: &AuthorizationContext, p: &str) -> bool {
    super::runtime::authorize(db, auth, p).is_ok()
}
pub(crate) fn rows(db: &Database, table: &str) -> Result<Vec<Vec<Value>>, QueryError> {
    let (auth, system) = crate::engine::expr::current_authorization_context().ok_or_else(|| {
        error(
            "ER_AI_ACCESS_DENIED",
            "AI metadata requires an authenticated session",
        )
    })?;
    let admin = system || can(db, &auth, "AI_ADMIN");
    if table != "ai_models" && !admin {
        return Err(error("ER_AI_ACCESS_DENIED", "AI_ADMIN required"));
    }
    let state = db.ai_catalog()?;
    let rows = match table {
        "ai_models" => state
            .models
            .iter()
            .filter(|(_, m)| {
                admin
                    || can(db, &auth, "AI_USE")
                    || can(db, &auth, &format!("AI_MODEL_{}_USE", m.id))
            })
            .map(|(name, m)| {
                let (schema, object) = name.rsplit_once('.').unwrap_or(("", name));
                vec![
                    Value::String(schema.into()),
                    Value::String(object.into()),
                    Value::UInt(m.id),
                    Value::String(m.kind.clone()),
                    Value::String(m.remote_model.clone()),
                    Value::String(m.model_revision.clone()),
                    Value::UInt(m.generation),
                    Value::Int(i64::from(m.enabled)),
                    m.embedding
                        .as_ref()
                        .map(|e| Value::UInt(e.dimensions as u64))
                        .unwrap_or(Value::Null),
                ]
            })
            .collect(),
        "ai_connections" => {
            let keys = load_keys(db.data_dir(), &state, false).ok();
            state
                .connections
                .iter()
                .map(|(name, c)| {
                    let available = c.auth.secret().is_none_or(|name| {
                        state.secrets.get(name).is_some_and(|s| {
                            keys.as_ref()
                                .is_some_and(|k| k.keys.contains_key(&s.key_id))
                        })
                    });
                    vec![
                        Value::String(name.clone()),
                        Value::String(c.protocol.clone()),
                        Value::String(c.base_url.clone()),
                        Value::UInt(c.generation),
                        Value::Int(i64::from(c.enabled)),
                        Value::Int(i64::from(available)),
                    ]
                })
                .collect()
        }
        "ai_secrets" => {
            let keys = load_keys(db.data_dir(), &state, false).ok();
            state
                .secrets
                .iter()
                .map(|(name, s)| {
                    vec![
                        Value::String(name.clone()),
                        Value::UInt(s.id),
                        Value::UInt(s.generation),
                        Value::UInt(s.crypto_generation),
                        Value::String(s.key_id.clone()),
                        Value::Int(i64::from(
                            keys.as_ref()
                                .is_some_and(|k| k.keys.contains_key(&s.key_id)),
                        )),
                    ]
                })
                .collect()
        }
        "ai_runtime_status" => super::runtime::status()
            .into_iter()
            .map(|(key, value)| vec![Value::String(key.into()), Value::UInt(value)])
            .collect(),
        _ => return Err(error("ER_AI_INVALID_ARGUMENT", "unknown AI metadata table")),
    };
    Ok(rows)
}
