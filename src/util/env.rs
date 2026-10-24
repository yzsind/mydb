use std::env::VarError;
use std::ffi::OsString;

/// Shared spelling rules for configuration booleans. Unknown values remain
/// distinguishable so each caller can retain its own default/error policy.
pub(crate) fn parse_bool(raw: &str) -> Option<bool> {
    let raw = raw.trim();
    if raw == "1"
        || ["on", "true", "yes"]
            .iter()
            .any(|word| raw.eq_ignore_ascii_case(word))
    {
        Some(true)
    } else if raw == "0"
        || ["off", "false", "no"]
            .iter()
            .any(|word| raw.eq_ignore_ascii_case(word))
    {
        Some(false)
    } else {
        None
    }
}

pub(crate) fn flag(name: &str, default: bool) -> bool {
    var(name)
        .map(|raw| parse_bool(&raw).unwrap_or(false))
        .unwrap_or(default)
}

#[cfg(test)]
mod bool_tests {
    use super::*;

    #[test]
    fn refactor_boolean_spelling_has_one_shared_contract() {
        for value in ["1", "on", "ON", "True", " yes "] {
            assert_eq!(parse_bool(value), Some(true), "{value}");
        }
        for value in ["0", "off", "Off", "false", " NO "] {
            assert_eq!(parse_bool(value), Some(false), "{value}");
        }
        assert_eq!(parse_bool("maybe"), None);
        assert_eq!(parse_bool(""), None);
    }
}

pub fn var(key: &str) -> Result<String, VarError> {
    #[cfg(test)]
    {
        if let Some(value) = test_override(key) {
            return value.ok_or(VarError::NotPresent);
        }
    }
    crate::startup_config::var(key)
}

pub fn var_os(key: &str) -> Option<OsString> {
    #[cfg(test)]
    {
        if let Some(value) = test_override(key) {
            return value.map(OsString::from);
        }
    }
    crate::startup_config::var_os(key)
}

#[cfg(test)]
thread_local! {
    static OVERRIDES: std::cell::RefCell<Vec<(&'static str, Option<String>)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
fn test_override(key: &str) -> Option<Option<String>> {
    OVERRIDES.with(|overrides| {
        overrides
            .borrow()
            .iter()
            .rev()
            .find_map(|(candidate, value)| (*candidate == key).then(|| value.clone()))
    })
}

#[cfg(test)]
pub(crate) type OverrideSnapshot = Vec<(&'static str, Option<String>)>;

#[cfg(not(test))]
#[derive(Clone)]
pub(crate) struct OverrideSnapshot;

/// Capture the effective test override stack so an execution worker observes
/// the same immutable configuration as the connection thread that planned
/// the statement. Production configuration is process-wide, so its snapshot
/// is intentionally zero-sized.
pub(crate) fn override_snapshot() -> OverrideSnapshot {
    #[cfg(test)]
    {
        return OVERRIDES.with(|overrides| overrides.borrow().clone());
    }
    #[cfg(not(test))]
    OverrideSnapshot
}

pub(crate) fn with_override_snapshot<R>(snapshot: OverrideSnapshot, f: impl FnOnce() -> R) -> R {
    #[cfg(test)]
    {
        let previous =
            OVERRIDES.with(|overrides| std::mem::replace(&mut *overrides.borrow_mut(), snapshot));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        OVERRIDES.with(|overrides| *overrides.borrow_mut() = previous);
        return match result {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        };
    }
    #[cfg(not(test))]
    {
        let _ = snapshot;
        f()
    }
}

#[cfg(test)]
pub fn with_vars<R>(vars: &[(&'static str, Option<&str>)], f: impl FnOnce() -> R) -> R {
    OVERRIDES.with(|overrides| {
        let mut overrides = overrides.borrow_mut();
        for (key, value) in vars {
            overrides.push((*key, value.map(str::to_owned)));
        }
    });

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));

    OVERRIDES.with(|overrides| {
        let mut overrides = overrides.borrow_mut();
        for _ in vars {
            overrides.pop();
        }
    });

    match result {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

/// Read a process-level boolean with the fulltext configuration's trimmed policy.
pub(crate) fn process_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "on" | "yes"
        ),
        Err(_) => default,
    }
}

/// Explicit false/off token in the statement's effective environment.
pub(crate) fn is_disabled(key: &str) -> bool {
    matches!(
        var(key).unwrap_or_default().to_ascii_lowercase().as_str(),
        "off" | "0" | "false" | "no"
    )
}
