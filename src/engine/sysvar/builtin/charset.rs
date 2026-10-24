//! Character set & collation variables — `character_set_*`,
//! `collation_*`. These are mostly per-session with `_database` reading
//! through to the current schema's stored charset.

use crate::engine::sysvar::backend::SysVar;
use crate::engine::sysvar::ctx::WriteCtx;
use crate::engine::sysvar::errors::wrong_value_for_var;
use crate::engine::sysvar::meta::{DataType, Scope, SysVarDef};
use crate::engine::sysvar::registry::Registry;
use crate::engine::sysvar::value::SysVarValue;
use crate::types::QueryError;

static SERVER_CHARSET: std::sync::RwLock<(&'static str, &'static str)> = std::sync::RwLock::new((
    crate::charset::DEFAULT_CHARSET,
    crate::charset::DEFAULT_COLLATION,
));

pub(crate) fn server_charset_defaults() -> (&'static str, &'static str) {
    *SERVER_CHARSET.read().unwrap()
}

fn global_block_encryption_mode_cell() -> &'static std::sync::RwLock<String> {
    static VALUE: std::sync::OnceLock<std::sync::RwLock<String>> = std::sync::OnceLock::new();
    VALUE.get_or_init(|| std::sync::RwLock::new("aes-128-ecb".to_string()))
}

pub(crate) fn global_block_encryption_mode() -> String {
    global_block_encryption_mode_cell()
        .read()
        .expect("global block_encryption_mode lock poisoned")
        .clone()
}

fn set_global_block_encryption_mode(value: String) {
    *global_block_encryption_mode_cell()
        .write()
        .expect("global block_encryption_mode lock poisoned") = value;
}

fn warn_deprecated_charset(charset: &str) {
    if charset.eq_ignore_ascii_case("utf8mb3") {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
            "'utf8mb3' is deprecated; use utf8mb4 instead",
        );
    }
}

fn warn_deprecated_collation(charset: &str) {
    if charset.eq_ignore_ascii_case("utf8mb3") {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_DEPRECATED_UTF8MB3_COLLATION,
            "The selected collation belongs to deprecated character set utf8mb3",
        );
    }
}

pub(crate) fn register(reg: &mut Registry) {
    // Client encodings have global connection defaults and session values.
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "character_set_client",
            Scope::Both,
            DataType::String { max_len: Some(64) },
        )
        .with_doc("Charset clients send statements in."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::String(s.character_set_client.clone()),
            None => SysVarValue::String(crate::charset::DEFAULT_CHARSET.to_string()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            let cs = crate::charset::resolve_client_charset(&raw)?;
            warn_deprecated_charset(cs.name);
            ctx.session.character_set_client = cs.name.to_string();
            Ok(())
        },
    ));
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "character_set_connection",
            Scope::Both,
            DataType::String { max_len: Some(64) },
        )
        .with_doc("Charset used for literal comparison."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::String(s.character_set_connection.clone()),
            None => SysVarValue::String(crate::charset::DEFAULT_CHARSET.to_string()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            // The connection repertoire includes server-side Unicode
            // charsets (utf16/utf32) that are intentionally not legal as a
            // client protocol encoding.
            let cs = crate::charset::resolve_charset(&raw)?;
            warn_deprecated_charset(cs.name);
            ctx.session.character_set_connection = cs.name.to_string();
            ctx.session.collation_connection =
                crate::charset::default_collation_for_charset(cs.name)?.to_string();
            Ok(())
        },
    ));
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "character_set_results",
            Scope::Both,
            DataType::String { max_len: Some(64) },
        )
        .with_doc("Charset the server sends result sets in (NULL = no conversion)."),
        |ctx| match ctx.session {
            Some(s) => match &s.character_set_results {
                Some(cs) => SysVarValue::String(cs.clone()),
                None => SysVarValue::Null,
            },
            None => SysVarValue::String(crate::charset::DEFAULT_CHARSET.to_string()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            if matches!(v, SysVarValue::Null) {
                ctx.session.character_set_results = None;
                return Ok(());
            }
            let raw = v.to_display_string();
            if raw.is_empty() || raw.eq_ignore_ascii_case("NULL") {
                ctx.session.character_set_results = None;
                return Ok(());
            }
            let cs = crate::charset::resolve_client_charset(&raw)?;
            warn_deprecated_charset(cs.name);
            ctx.session.character_set_results = Some(cs.name.to_string());
            Ok(())
        },
    ));
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "collation_connection",
            Scope::Both,
            DataType::String { max_len: Some(64) },
        )
        .with_doc("Collation used for literal comparison."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::String(s.collation_connection.clone()),
            None => SysVarValue::String(crate::charset::DEFAULT_COLLATION.to_string()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            let col = crate::charset::resolve_collation(&raw)?;
            crate::charset::resolve_charset(col.charset)?;
            warn_deprecated_collation(col.charset);
            ctx.session.collation_connection = col.name.to_string();
            ctx.session.character_set_connection = col.charset.to_string();
            Ok(())
        },
    ));
    // These variables describe one default encoding, so updates keep the
    // charset/collation pair coherent in both scopes.
    for (name, charset_variable) in [("character_set_server", true), ("collation_server", false)] {
        reg.register(
            SysVar::settable(
                SysVarDef::new(name, Scope::Both, DataType::String { max_len: Some(64) })
                    .with_doc("Server default encoding for new databases."),
                move |ctx| {
                    let value = match (ctx.scope, ctx.session) {
                        (Scope::Session, Some(s)) if charset_variable => {
                            s.sysvars.character_set_server.clone()
                        }
                        (Scope::Session, Some(s)) => s.sysvars.collation_server.clone(),
                        _ => {
                            let pair = server_charset_defaults();
                            if charset_variable { pair.0 } else { pair.1 }.to_string()
                        }
                    };
                    SysVarValue::String(value)
                },
                move |ctx, value| {
                    let raw = value.to_display_string();
                    let pair = if charset_variable {
                        let charset = crate::charset::resolve_charset(&raw)?;
                        warn_deprecated_charset(charset.name);
                        (
                            charset.name,
                            crate::charset::default_collation_for_charset(charset.name)?,
                        )
                    } else {
                        let collation = crate::charset::resolve_collation(&raw)?;
                        warn_deprecated_collation(collation.charset);
                        (collation.charset, collation.name)
                    };
                    if ctx.is_global() {
                        *SERVER_CHARSET.write().unwrap() = pair;
                    } else {
                        ctx.session.sysvars.character_set_server = pair.0.to_string();
                        ctx.session.sysvars.collation_server = pair.1.to_string();
                    }
                    Ok(())
                },
            )
            .with_validation(move |_, value| {
                let raw = value.to_display_string();
                if charset_variable {
                    let charset = crate::charset::resolve_charset(&raw)?;
                    crate::charset::default_collation_for_charset(charset.name)?;
                } else {
                    crate::charset::resolve_collation(&raw)?;
                }
                Ok(())
            })
            .with_default(SysVarValue::String(
                if charset_variable {
                    crate::charset::DEFAULT_CHARSET
                } else {
                    crate::charset::DEFAULT_COLLATION
                }
                .to_string(),
            )),
        );
    }

    // character_set_database / collation_database — derive from the current
    // database's stored charset/collation; settable in MySQL 8 but
    // deprecated. We accept SET and stash the override on the session.
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "character_set_database",
            Scope::Both,
            DataType::String { max_len: Some(64) },
        )
        .with_doc("Charset of the default database for this session."),
        |ctx| match ctx.session {
            Some(s) => {
                if let Some(override_cs) = &s.sysvars.character_set_database {
                    SysVarValue::String(override_cs.clone())
                } else if let Some(db) = ctx.db {
                    SysVarValue::String(
                        db.schema_charset(&s.current_database)
                            .unwrap_or_else(|| crate::charset::DEFAULT_CHARSET.to_string()),
                    )
                } else {
                    SysVarValue::String(crate::charset::DEFAULT_CHARSET.to_string())
                }
            }
            None => SysVarValue::String(crate::charset::DEFAULT_CHARSET.to_string()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            let cs = crate::charset::resolve_client_charset(&raw)?;
            warn_deprecated_charset(cs.name);
            ctx.session.sysvars.character_set_database = Some(cs.name.to_string());
            Ok(())
        },
    ));
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "collation_database",
            Scope::Both,
            DataType::String { max_len: Some(64) },
        )
        .with_doc("Collation of the default database for this session."),
        |ctx| match ctx.session {
            Some(s) => {
                if let Some(override_co) = &s.sysvars.collation_database {
                    SysVarValue::String(override_co.clone())
                } else if let Some(db) = ctx.db {
                    SysVarValue::String(
                        db.schema_collation(&s.current_database)
                            .unwrap_or_else(|| crate::charset::DEFAULT_COLLATION.to_string()),
                    )
                } else {
                    SysVarValue::String(crate::charset::DEFAULT_COLLATION.to_string())
                }
            }
            None => SysVarValue::String(crate::charset::DEFAULT_COLLATION.to_string()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            let col = crate::charset::resolve_collation(&raw)?;
            warn_deprecated_collation(col.charset);
            ctx.session.sysvars.collation_database = Some(col.name.to_string());
            Ok(())
        },
    ));

    // character_set_filesystem — Both. Used by LOAD DATA INFILE for
    // interpreting filename bytes. mydb keeps it as a passthrough string.
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "character_set_filesystem",
            Scope::Both,
            DataType::String { max_len: Some(64) },
        )
        .with_doc("Charset filenames passed to LOAD DATA INFILE are interpreted in."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::String(s.sysvars.character_set_filesystem.clone()),
            None => SysVarValue::String("binary".to_string()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            // accept any recognised charset name; default is `binary`.
            if raw.eq_ignore_ascii_case("binary") {
                ctx.session.sysvars.character_set_filesystem = "binary".to_string();
            } else {
                let cs = crate::charset::resolve_client_charset(&raw)?;
                warn_deprecated_charset(cs.name);
                ctx.session.sysvars.character_set_filesystem = cs.name.to_string();
            }
            Ok(())
        },
    ));

    // character_set_system — read-only. MySQL keeps it at `utf8mb3`.
    reg.register(SysVar::const_read_only(
        SysVarDef::new(
            "character_set_system",
            Scope::Global,
            DataType::String { max_len: Some(64) },
        )
        .with_doc("Charset metadata is stored in (read-only)."),
        SysVarValue::String("utf8mb3".to_string()),
    ));

    // SET NAMES is dispatched via a dedicated parser node; the registry
    // does not need to expose it.

    // Block-encryption mode used by AES_ENCRYPT / AES_DECRYPT.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "block_encryption_mode",
            Scope::Both,
            DataType::String { max_len: Some(64) },
        )
        .with_doc("Block-encryption mode for the AES_ENCRYPT family."),
        |ctx| {
            if ctx.scope == Scope::Global {
                SysVarValue::String(global_block_encryption_mode())
            } else {
                ctx.session
                    .map(|s| SysVarValue::String(s.sysvars.block_encryption_mode.clone()))
                    .unwrap_or_else(|| SysVarValue::String(global_block_encryption_mode()))
            }
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            if validate_block_encryption_mode(&raw) {
                let canonical = raw.to_ascii_lowercase();
                if ctx.is_global() {
                    set_global_block_encryption_mode(canonical);
                } else {
                    ctx.session.sysvars.block_encryption_mode = canonical;
                }
                Ok(())
            } else {
                Err(wrong_value_for_var("block_encryption_mode", &raw))
            }
        },
    ));
}

fn validate_block_encryption_mode(raw: &str) -> bool {
    // Single source of truth with the AES evaluator (adds ctr to the
    // historic list).
    crate::engine::expr::parse_block_encryption_mode(raw).is_some()
}
