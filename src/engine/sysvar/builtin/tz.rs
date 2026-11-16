//! Time-zone variables — `time_zone` (session) and `system_time_zone`
//! (read-only system probe).

use std::sync::{
    atomic::{AtomicBool, Ordering},
    OnceLock, RwLock,
};

use crate::engine::sysvar::backend::SysVar;
use crate::engine::sysvar::ctx::WriteCtx;
use crate::engine::sysvar::errors::wrong_value_for_var;
use crate::engine::sysvar::meta::{DataType, Scope, SysVarDef};
use crate::engine::sysvar::registry::Registry;
use crate::engine::sysvar::value::SysVarValue;
use crate::types::QueryError;

pub(crate) fn register(reg: &mut Registry) {
    reg.register(
        SysVar::settable(
            SysVarDef::new(
                "time_zone",
                Scope::Both,
                DataType::String { max_len: Some(64) },
            )
            .with_doc("Session time zone — `SYSTEM`, `+HH:MM`, `-HH:MM`, or a named zone."),
            |ctx| match ctx.scope {
                Scope::Global => SysVarValue::String(global_time_zone()),
                Scope::Session | Scope::Both => ctx
                    .session
                    .map(|s| SysVarValue::String(s.sysvars.time_zone.clone()))
                    .unwrap_or_else(|| SysVarValue::String(global_time_zone())),
            },
            |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                let raw = v.to_display_string();
                if !validate_time_zone(&raw) {
                    return Err(crate::error::catalog::ER_UNKNOWN_TIME_ZONE
                        .error(format!("Unknown or incorrect time zone: '{raw}'")));
                }
                let canonical = canonical_time_zone(&raw);
                match ctx.scope {
                    Scope::Global => set_global_time_zone(canonical),
                    Scope::Session => ctx.session.sysvars.time_zone = canonical,
                    Scope::Both => unreachable!(),
                }
                Ok(())
            },
        )
        .with_validation(|_, value| {
            let raw = value.to_display_string();
            if !validate_time_zone(&raw) {
                return Err(crate::error::catalog::ER_UNKNOWN_TIME_ZONE
                    .error(format!("Unknown or incorrect time zone: '{raw}'")));
            }
            Ok(())
        }),
    );

    reg.register(SysVar::runtime_read_only(
        SysVarDef::new(
            "system_time_zone",
            Scope::Global,
            DataType::String { max_len: Some(64) },
        )
        .with_doc("Process-wide system time zone snapshot."),
        || SysVarValue::String(detect_system_time_zone()),
    ));

    // `explicit_defaults_for_timestamp` — MySQL 8.0 default ON while still
    // retaining the legacy OFF mode on a per-session basis.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "explicit_defaults_for_timestamp",
            Scope::Both,
            DataType::Bool,
        )
        .with_doc("Require explicit TIMESTAMP DEFAULT clauses."),
        |ctx| match ctx.scope {
            Scope::Global => SysVarValue::Bool(global_explicit_defaults_for_timestamp()),
            Scope::Session | Scope::Both => ctx
                .session
                .map(|session| SysVarValue::Bool(session.sysvars.explicit_defaults_for_timestamp))
                .unwrap_or_else(|| SysVarValue::Bool(global_explicit_defaults_for_timestamp())),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let enabled = v.as_bool().unwrap_or(false);
            if !enabled {
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                    "'explicit_defaults_for_timestamp' is deprecated and will be removed in a future release",
                );
            }
            match ctx.scope {
                Scope::Global => {
                    GLOBAL_EXPLICIT_DEFAULTS_FOR_TIMESTAMP.store(enabled, Ordering::Release)
                }
                Scope::Session => ctx.session.sysvars.explicit_defaults_for_timestamp = enabled,
                Scope::Both => unreachable!(),
            }
            Ok(())
        },
    ));

    // `timestamp` — per-session epoch override (seconds, fractional part
    // allowed; 0 = use the real clock). Drives NOW()/CURDATE()/CURTIME()/
    // UTC_*/UNIX_TIMESTAMP(); SYSDATE() stays on the real clock.
    reg.register(
        SysVar::settable(
            SysVarDef::new(
                "timestamp",
                Scope::Session,
                DataType::Double {
                    // Syntax/type coercion belongs to the generic sysvar layer.
                    // The extended timestamp boundary is enforced by
                    // the setter below so an out-of-range numeric value reports
                    // ER_WRONG_VALUE_FOR_VAR (1231), not a conversion error.
                    min: f64::MIN,
                    max: f64::MAX,
                },
            )
            .with_doc("Session statement-time override for NOW() and friends; 0 = real clock."),
            |ctx| match ctx.session {
                Some(s) => {
                    let v = s.sysvars.timestamp_override;
                    if v == 0.0 {
                        let now = chrono::Utc::now();
                        SysVarValue::Double(
                            now.timestamp() as f64 + now.timestamp_subsec_micros() as f64 / 1e6,
                        )
                    } else {
                        SysVarValue::Double(v)
                    }
                }
                None => SysVarValue::Double(0.0),
            },
            |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                let raw = match v {
                    SysVarValue::Double(d) => d,
                    SysVarValue::Int(i) => i as f64,
                    SysVarValue::UInt(u) => u as f64,
                    _ => return Err(wrong_value_for_var("timestamp", &v.to_display_string())),
                };
                if !raw.is_finite()
                    || raw < i32::MIN as f64
                    || raw >= (crate::types::MAX_TIMESTAMP_SECONDS + 1) as f64
                {
                    return Err(wrong_value_for_var("timestamp", &raw.to_string()));
                }
                ctx.session.sysvars.timestamp_override = raw;
                Ok(())
            },
        )
        .with_validation(|_, value| {
            if let SysVarValue::Double(raw) = value {
                if !raw.is_finite()
                    || *raw < i32::MIN as f64
                    || *raw >= (crate::types::MAX_TIMESTAMP_SECONDS + 1) as f64
                {
                    return Err(wrong_value_for_var("timestamp", &raw.to_string()));
                }
            }
            Ok(())
        }),
    );
}

static GLOBAL_TIME_ZONE: OnceLock<RwLock<String>> = OnceLock::new();
static GLOBAL_EXPLICIT_DEFAULTS_FOR_TIMESTAMP: AtomicBool = AtomicBool::new(true);

pub(crate) fn global_explicit_defaults_for_timestamp() -> bool {
    GLOBAL_EXPLICIT_DEFAULTS_FOR_TIMESTAMP.load(Ordering::Acquire)
}

fn global_time_zone_cell() -> &'static RwLock<String> {
    GLOBAL_TIME_ZONE.get_or_init(|| RwLock::new("SYSTEM".to_string()))
}

pub(crate) fn global_time_zone() -> String {
    global_time_zone_cell().read().unwrap().clone()
}

fn set_global_time_zone(value: String) {
    *global_time_zone_cell().write().unwrap() = value;
}

/// Accept `SYSTEM`, a MySQL-valid signed offset, or an IANA zone that can be
/// resolved from the host zoneinfo catalog. Validation happens at SET time so
/// every temporal function observes the same success/error semantics.
pub fn validate_time_zone(raw: &str) -> bool {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("SYSTEM") {
        return true;
    }
    if trimmed.eq_ignore_ascii_case("UTC") || trimmed.eq_ignore_ascii_case("GMT") {
        return true;
    }
    if parse_signed_offset(trimmed).is_some() {
        return true;
    }
    crate::engine::tz::find_named_zone(trimmed).is_some()
}

pub fn canonical_time_zone(raw: &str) -> String {
    if raw.trim().eq_ignore_ascii_case("SYSTEM") {
        return "SYSTEM".to_string();
    }
    if let Some((sign, h, m)) = parse_signed_offset(raw.trim()) {
        return format!("{}{:02}:{:02}", sign, h, m);
    }
    raw.trim().to_string()
}

fn parse_signed_offset(raw: &str) -> Option<(char, u32, u32)> {
    let bytes = raw.as_bytes();
    if bytes.len() < 5 {
        return None;
    }
    let sign = bytes[0] as char;
    if sign != '+' && sign != '-' {
        return None;
    }
    let rest = &raw[1..];
    let (hh, mm) = rest.split_once(':')?;
    let h: u32 = hh.parse().ok()?;
    let m: u32 = mm.parse().ok()?;
    // MySQL permits offsets from -13:59 through +14:00.
    if m > 59 || h > 14 || (h == 14 && (sign == '-' || m != 0)) {
        return None;
    }
    Some((sign, h, m))
}

static SYSTEM_TZ: OnceLock<String> = OnceLock::new();

pub fn detect_system_time_zone() -> String {
    SYSTEM_TZ
        .get_or_init(|| {
            if let Ok(tz) = std::env::var("TZ") {
                if !tz.trim().is_empty() {
                    return tz;
                }
            }
            for path in ["/etc/localtime", "/var/db/timezone/localtime"] {
                if let Ok(target) = std::fs::read_link(path) {
                    let text = target.to_string_lossy();
                    if let Some((_, zone)) = text.split_once("zoneinfo/") {
                        if !zone.is_empty() {
                            return zone.to_string();
                        }
                    }
                }
            }
            chrono::Local::now().format("%Z").to_string()
        })
        .clone()
}

#[cfg(test)]
/// Parse the configured session time zone into a UTC offset in seconds.
/// `SYSTEM` returns the process time zone offset; named IANA zones
/// resolve through the host zoneinfo database (`crate::engine::tz`) at
/// the current instant; unknown names return `None`.
pub fn session_offset_seconds(tz: &str) -> Option<i64> {
    session_offset_seconds_at(tz, chrono::Utc::now().timestamp())
}

/// Like [`session_offset_seconds`] but evaluated at a specific UTC
/// instant — named zones with DST resolve the offset in effect *then*,
/// which matters when `SET timestamp` rewinds the session clock.
pub fn session_offset_seconds_at(tz: &str, utc_epoch: i64) -> Option<i64> {
    if tz.eq_ignore_ascii_case("SYSTEM") {
        return system_offset_seconds_at(utc_epoch);
    }
    if tz.eq_ignore_ascii_case("UTC") || tz.eq_ignore_ascii_case("GMT") {
        return Some(0);
    }
    if let Some((sign, h, m)) = parse_signed_offset(tz) {
        let s = (h as i64) * 3600 + (m as i64) * 60;
        return Some(if sign == '-' { -s } else { s });
    }
    let zone = crate::engine::tz::find_named_zone(tz)?;
    Some(zone.offset_at_utc(utc_epoch) as i64)
}

fn system_offset_seconds_at(utc_epoch: i64) -> Option<i64> {
    chrono::DateTime::from_timestamp(utc_epoch, 0)
        .map(|utc| utc.with_timezone(&chrono::Local).offset().local_minus_utc() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_canonical_forms() {
        assert!(validate_time_zone("SYSTEM"));
        assert!(validate_time_zone("+08:00"));
        assert!(validate_time_zone("-05:00"));
        assert!(validate_time_zone("UTC"));
        assert!(validate_time_zone("Asia/Shanghai"));
    }

    #[test]
    fn rejects_garbage() {
        assert!(!validate_time_zone(""));
        assert!(!validate_time_zone("!@#"));
        assert!(!validate_time_zone("-14:00"));
        assert!(!validate_time_zone("+14:01"));
        assert!(validate_time_zone("-13:59"));
        assert!(validate_time_zone("+14:00"));
    }

    #[test]
    fn canonicalises_offsets() {
        assert_eq!(canonical_time_zone("+8:00"), "+08:00");
        assert_eq!(canonical_time_zone("-05:00"), "-05:00");
    }

    #[test]
    fn session_offset_for_fixed_zone() {
        assert_eq!(session_offset_seconds("+08:00"), Some(8 * 3600));
        assert_eq!(
            session_offset_seconds("-05:30"),
            Some(-(5 * 3600 + 30 * 60))
        );
    }
}
