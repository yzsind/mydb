//! `validate_password` component (MySQL 8.0 compatible).
//!
//! Stock MySQL performs no password-strength checking unless the
//! `validate_password` component is installed (`INSTALL COMPONENT
//! 'file://component_validate_password'`). mydb mirrors that: enforcement is
//! **off by default** and turned on by installing the component. Once
//! installed, the `validate_password.*` system variables drive a real policy
//! that `CREATE USER` / `ALTER USER` / `SET PASSWORD` consult on the plaintext
//! password, rejecting weak passwords with error 1819. `IDENTIFIED WITH ...
//! AS '<hash>'` (pre-hashed) paths are exempt, exactly as in MySQL.
//!
//! Keeping the component opt-in preserves MySQL default behaviour (no
//! surprise rejections) while ensuring the `validate_password.*` variables are
//! not silent no-ops — they steer enforcement whenever the component is on.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};

use crate::storage::error;
use crate::types::QueryError;

/// The component's canonical URN, plus the bare name people also type.
pub const COMPONENT_URN: &str = "file://component_validate_password";

static INSTALLED: AtomicBool = AtomicBool::new(false);
// 0 = LOW, 1 = MEDIUM, 2 = STRONG. MySQL default MEDIUM.
static POLICY: AtomicU64 = AtomicU64::new(1);
static LENGTH: AtomicU64 = AtomicU64::new(8);
static MIXED_CASE_COUNT: AtomicU64 = AtomicU64::new(1);
static NUMBER_COUNT: AtomicU64 = AtomicU64::new(1);
static SPECIAL_CHAR_COUNT: AtomicU64 = AtomicU64::new(1);
static CHECK_USER_NAME: AtomicBool = AtomicBool::new(true);

pub fn is_installed() -> bool {
    INSTALLED.load(Ordering::Relaxed)
}

pub fn set_installed(on: bool) {
    INSTALLED.store(on, Ordering::Relaxed);
}

/// True if `name` denotes the validate_password component in any of the forms
/// MySQL accepts (`file://component_validate_password`, with or without the
/// `file://` prefix or a `.so` suffix, case-insensitive).
pub fn is_validate_password_component(name: &str) -> bool {
    let n = name.trim().trim_start_matches("file://");
    let n = n.strip_suffix(".so").unwrap_or(n);
    n.eq_ignore_ascii_case("component_validate_password")
}

pub fn policy() -> u64 {
    POLICY.load(Ordering::Relaxed)
}

pub fn policy_str() -> &'static str {
    match policy() {
        0 => "LOW",
        2 => "STRONG",
        _ => "MEDIUM",
    }
}

/// Parse a `validate_password.policy` value (`LOW`/`MEDIUM`/`STRONG` or
/// `0`/`1`/`2`). Returns the numeric code or an error for anything else.
pub fn parse_policy(value: &str) -> Result<u64, QueryError> {
    match value.trim().to_ascii_uppercase().as_str() {
        "LOW" | "0" => Ok(0),
        "MEDIUM" | "1" => Ok(1),
        "STRONG" | "2" => Ok(2),
        other => Err(error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
            format!("Variable 'validate_password.policy' can't be set to the value of '{other}'"),
        )),
    }
}

pub fn set_policy(code: u64) {
    POLICY.store(code, Ordering::Relaxed);
}

pub fn length() -> u64 {
    LENGTH.load(Ordering::Relaxed)
}
pub fn set_length(v: u64) {
    LENGTH.store(v, Ordering::Relaxed);
}
pub fn mixed_case_count() -> u64 {
    MIXED_CASE_COUNT.load(Ordering::Relaxed)
}
pub fn set_mixed_case_count(v: u64) {
    MIXED_CASE_COUNT.store(v, Ordering::Relaxed);
}
pub fn number_count() -> u64 {
    NUMBER_COUNT.load(Ordering::Relaxed)
}
pub fn set_number_count(v: u64) {
    NUMBER_COUNT.store(v, Ordering::Relaxed);
}
pub fn special_char_count() -> u64 {
    SPECIAL_CHAR_COUNT.load(Ordering::Relaxed)
}
pub fn set_special_char_count(v: u64) {
    SPECIAL_CHAR_COUNT.store(v, Ordering::Relaxed);
}
pub fn check_user_name() -> bool {
    CHECK_USER_NAME.load(Ordering::Relaxed)
}
pub fn set_check_user_name(v: bool) {
    CHECK_USER_NAME.store(v, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// authentication_policy (MySQL 8.0.27+). mydb authenticates with a single
// factor, so the multi-factor *constraint* isn't enforced — but the variable
// is validated and round-trips (SHOW/@@ reflect it), and setting it raises a
// deprecation-style notice that the multi-factor policy is accepted-not-
// enforced. This keeps it from being a silent no-op while remaining honest
// about the MFA gap (tracked separately).
// ---------------------------------------------------------------------------

fn auth_policy_cell() -> &'static RwLock<String> {
    static CELL: OnceLock<RwLock<String>> = OnceLock::new();
    CELL.get_or_init(|| RwLock::new("*,,".to_string()))
}

pub fn authentication_policy() -> String {
    auth_policy_cell().read().unwrap().clone()
}

/// Validate and store an `authentication_policy` value. MySQL requires 1–3
/// comma-separated factors whose first element is non-empty (a plugin name or
/// `*`). Malformed values are rejected with 1231 (not silently accepted).
pub fn set_authentication_policy(value: &str) -> Result<(), QueryError> {
    let parts: Vec<&str> = value.split(',').collect();
    let bad = || {
        error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
            format!("Variable 'authentication_policy' can't be set to the value of '{value}'"),
        )
    };
    if parts.is_empty() || parts.len() > 3 {
        return Err(bad());
    }
    if parts[0].trim().is_empty() {
        // The first factor is mandatory in MySQL.
        return Err(bad());
    }
    *auth_policy_cell().write().unwrap() = value.to_string();
    Ok(())
}

fn too_weak() -> QueryError {
    error(
        crate::error::catalog::ER_NOT_VALID_PASSWORD,
        "Your password does not satisfy the current policy requirements",
    )
}

/// Enforce the active policy on a plaintext `password`. No-op (Ok) when the
/// component is not installed. `user_name` feeds the `check_user_name` rule
/// (a password equal to the account's user name is rejected).
pub fn validate(password: &str, user_name: Option<&str>) -> Result<(), QueryError> {
    if !is_installed() {
        return Ok(());
    }
    // LOW and up: minimum length.
    if (password.chars().count() as u64) < length() {
        return Err(too_weak());
    }
    // check_user_name: reject a password identical to the account's user part.
    if check_user_name() {
        if let Some(user) = user_name {
            if !user.is_empty() && password.eq_ignore_ascii_case(user) {
                return Err(too_weak());
            }
        }
    }
    // MEDIUM and up: digit / mixed-case / special-character minimums.
    if policy() >= 1 {
        let digits = password.chars().filter(|c| c.is_ascii_digit()).count() as u64;
        let lowers = password.chars().filter(|c| c.is_ascii_lowercase()).count() as u64;
        let uppers = password.chars().filter(|c| c.is_ascii_uppercase()).count() as u64;
        let specials = password
            .chars()
            .filter(|c| !c.is_alphanumeric() && !c.is_whitespace())
            .count() as u64;
        let mc = mixed_case_count();
        if digits < number_count() || specials < special_char_count() || lowers < mc || uppers < mc
        {
            return Err(too_weak());
        }
    }
    // STRONG additionally checks a dictionary file; mydb ships no dictionary,
    // so STRONG behaves as MEDIUM (documented limitation).
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn off_by_default_accepts_anything() {
        set_installed(false);
        assert!(validate("x", Some("root")).is_ok());
    }

    #[test]
    fn medium_policy_enforces_composition() {
        set_installed(true);
        set_policy(1);
        set_length(8);
        set_mixed_case_count(1);
        set_number_count(1);
        set_special_char_count(1);
        // too short
        assert_eq!(validate("Ab1!x", None).unwrap_err().code(), 1819);
        // no special / digit
        assert_eq!(validate("Abcdefgh", None).unwrap_err().code(), 1819);
        // satisfies all
        assert!(validate("Abcdef1!", None).is_ok());
        set_installed(false);
    }

    #[test]
    fn component_name_matching() {
        assert!(is_validate_password_component(
            "file://component_validate_password"
        ));
        assert!(is_validate_password_component(
            "component_validate_password"
        ));
        assert!(is_validate_password_component(
            "component_validate_password.so"
        ));
        assert!(!is_validate_password_component("component_audit_log"));
    }
}
