//! System variable metadata — `Scope`, `Access`, `DataType`, and
//! `SysVarDef`. The registry stores a `&'static SysVarDef` per variable
//! alongside its read / write function pointers (see `backend.rs`).

use crate::types::QueryError;

use super::errors::{
    global_only_variable, out_of_range, read_only_variable, session_only_variable,
    wrong_type_for_var, wrong_value_for_var,
};
use super::value::{format_double, parse_bool_sysvar, parse_int_sysvar, SysVarValue};

/// Where a variable lives, and which `SET` forms are allowed.
///
/// * `Both` — has both a global default *and* per-session value. Bare `SET`
///   modifies the session; `SET GLOBAL` modifies the global. This is the
///   common case (autocommit, sql_mode, character_set_client, ...).
/// * `Session` — exists only per session (no settable global). Bare `SET`
///   and `SET SESSION` work; `SET GLOBAL` returns ER_LOCAL_VARIABLE.
/// * `Global` — exists only globally (no per-session override). Bare `SET`
///   and `SET SESSION` return ER_GLOBAL_VARIABLE; only `SET GLOBAL` works.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Both,
    Session,
    Global,
}

impl Scope {
    pub fn allows_global(self) -> bool {
        matches!(self, Scope::Global | Scope::Both)
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Both => "BOTH",
            Scope::Session => "SESSION",
            Scope::Global => "GLOBAL",
        }
    }
}

/// Set scope as parsed from the SQL statement. We translate the parser's
/// `SetScope` into this richer enum so the dispatcher can distinguish a
/// bare `SET x = y` (default — Session for `Both`-scope vars) from an
/// explicit `SET SESSION` or `SET GLOBAL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetScope {
    /// `SET x = y` — defaults to Session for `Both`-scope vars.
    Default,
    Session,
    Global,
    /// `SET PERSIST x = y` — update the runtime global value AND write the
    /// new value to `mysqld-auto.cnf` so it survives a restart.
    Persist,
    /// `SET PERSIST_ONLY x = y` — only write the value to `mysqld-auto.cnf`;
    /// the running value is left alone. The only `SET` form that is
    /// permitted on `ReadOnly` variables (per MySQL 8.0).
    PersistOnly,
}

impl SetScope {
    pub fn from_parser(scope: Option<crate::parser::SetScope>) -> SetScope {
        match scope {
            None => SetScope::Default,
            Some(crate::parser::SetScope::Session) => SetScope::Session,
            Some(crate::parser::SetScope::Global) => SetScope::Global,
            Some(crate::parser::SetScope::Persist) => SetScope::Persist,
            Some(crate::parser::SetScope::PersistOnly) => SetScope::PersistOnly,
        }
    }

    /// True for `SET PERSIST` / `SET PERSIST_ONLY` — caller must record the
    /// value in `mysqld-auto.cnf`.
    pub fn is_persist(self) -> bool {
        matches!(self, SetScope::Persist | SetScope::PersistOnly)
    }

    /// True only for `SET PERSIST_ONLY` — the runtime value MUST be left
    /// untouched.
    pub fn is_persist_only(self) -> bool {
        matches!(self, SetScope::PersistOnly)
    }

    /// Scope named by the SET syntax before checking whether the variable
    /// actually exposes that scope. This is used for MySQL's error
    /// precedence: a read-only variable reports 1238 before a global-only
    /// variable reports the bare-SET scope error 1229.
    pub fn requested_scope(self) -> Scope {
        match self {
            SetScope::Default | SetScope::Session => Scope::Session,
            SetScope::Global | SetScope::Persist | SetScope::PersistOnly => Scope::Global,
        }
    }

    /// Effective scope after applying `Default → Session` for vars that
    /// allow session. Returns Err with the appropriate MySQL error for
    /// scope mismatches.
    ///
    /// `PERSIST` and `PERSIST_ONLY` both target the GLOBAL value — MySQL
    /// rejects them on Session-only variables (ER_LOCAL_VARIABLE 1228).
    pub fn resolve_for(self, name: &str, var_scope: Scope) -> Result<Scope, QueryError> {
        match (self, var_scope) {
            // Bare SET on a Global-only var.
            (SetScope::Default, Scope::Global) => Err(global_only_variable(name)),
            (SetScope::Session, Scope::Global) => Err(global_only_variable(name)),
            (SetScope::Global, Scope::Session) => Err(session_only_variable(name)),
            // PERSIST / PERSIST_ONLY are GLOBAL operations.
            (SetScope::Persist, Scope::Session) | (SetScope::PersistOnly, Scope::Session) => {
                Err(session_only_variable(name))
            }
            (SetScope::Default, _) | (SetScope::Session, _) => Ok(Scope::Session),
            (SetScope::Global, _) | (SetScope::Persist, _) | (SetScope::PersistOnly, _) => {
                Ok(Scope::Global)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Fully settable at runtime.
    ReadWrite,
    /// Set at server startup only — `SET` returns ER_READ_ONLY_VARIABLE.
    ReadOnly,
    /// Requires `SYSTEM_VARIABLES_ADMIN` (or equivalent). For mydb this
    /// is enforced once the auth layer wires the check in; until then
    /// behaves like `ReadWrite`.
    SuperReadWrite,
}

/// The space of values a variable accepts. `DataType::coerce` does the
/// MySQL-style permissive conversion (`'1'` → bool, `'OFF'` → bool, etc.).
#[derive(Debug, Clone)]
pub enum DataType {
    Bool,
    Int {
        min: i64,
        max: i64,
    },
    UInt {
        min: u64,
        max: u64,
    },
    Double {
        min: f64,
        max: f64,
    },
    /// Single-pick enum. First name is the canonical form, the rest are
    /// accepted aliases.
    Enum {
        values: &'static [&'static str],
    },
    /// MySQL `SET` type — comma-separated subset of `values`. Empty input
    /// (the empty string) is a valid value meaning "no options".
    SetType {
        values: &'static [&'static str],
    },
    /// Free-form string. `max_len` is enforced if `Some`.
    String {
        max_len: Option<usize>,
    },
    /// A string or NULL, rejecting implicit conversion from numeric/boolean
    /// SQL values like MySQL's string system variables.
    StrictString {
        max_len: Option<usize>,
        /// Some MySQL string variables normalize SQL NULL to an empty string
        /// before storing either runtime or persisted configuration.
        null_as_empty: bool,
    },
}

impl DataType {
    pub fn coerce(&self, name: &str, raw: SysVarValue) -> Result<SysVarValue, QueryError> {
        if matches!(self, DataType::StrictString { .. })
            && !matches!(raw, SysVarValue::String(_) | SysVarValue::Null)
        {
            return Err(wrong_type_for_var(name));
        }
        match self {
            DataType::Bool => match raw {
                SysVarValue::Bool(b) => Ok(SysVarValue::Bool(b)),
                // MySQL boolean sysvars accept only 0/1 as integers; any other
                // integer (2, -1, 1027, ...) is ER_WRONG_VALUE_FOR_VAR (1231).
                // Bareword TRUE/FALSE reach here as Int(1)/Int(0).
                SysVarValue::Int(i) => match i {
                    0 => Ok(SysVarValue::Bool(false)),
                    1 => Ok(SysVarValue::Bool(true)),
                    _ => Err(wrong_value_for_var(name, &i.to_string())),
                },
                SysVarValue::UInt(u) => match u {
                    0 => Ok(SysVarValue::Bool(false)),
                    1 => Ok(SysVarValue::Bool(true)),
                    _ => Err(wrong_value_for_var(name, &u.to_string())),
                },
                // String form accepts only ON/OFF/0/1 — NOT 'yes'/'no'/'true'/
                // 'false' (those are legacy config-file spellings, and MySQL
                // rejects the quoted forms for boolean sysvars).
                SysVarValue::String(ref s) => parse_bool_sysvar(s)
                    .map(SysVarValue::Bool)
                    .map_err(|_| wrong_value_for_var(name, s)),
                _ => Err(wrong_value_for_var(name, &raw.to_display_string())),
            },
            DataType::Int { min, max } => {
                let n: i128 = match &raw {
                    SysVarValue::Int(i) => *i as i128,
                    SysVarValue::UInt(u) => *u as i128,
                    SysVarValue::Bool(b) => i128::from(*b),
                    // Approximate-value literals (`1e1` → Double, `1.1`/`65530.34`
                    // → Decimal-as-string) are a *type* error for integer
                    // sysvars in MySQL — ER_WRONG_TYPE_FOR_VAR (1232) — even when
                    // the value is integral (`1e1` == 10). Fractional strings and
                    // non-numeric strings ('foo', ' ') fall through to the string
                    // arm below and hit the same 1232.
                    SysVarValue::Double(_) => return Err(wrong_type_for_var(name)),
                    SysVarValue::String(s) => {
                        parse_int_sysvar(s).ok_or_else(|| wrong_type_for_var(name))?
                    }
                    _ => return Err(wrong_value_for_var(name, &raw.to_display_string())),
                };
                if n < i128::from(*min) || n > i128::from(*max) {
                    return Err(out_of_range(name, &n.to_string()));
                }
                Ok(SysVarValue::Int(n as i64))
            }
            DataType::UInt { min, max } => {
                let n: i128 = match &raw {
                    SysVarValue::UInt(u) => *u as i128,
                    SysVarValue::Int(i) => *i as i128,
                    SysVarValue::Bool(b) => i128::from(*b),
                    SysVarValue::Double(_) => return Err(wrong_type_for_var(name)),
                    SysVarValue::String(s) => {
                        parse_int_sysvar(s).ok_or_else(|| wrong_type_for_var(name))?
                    }
                    _ => return Err(wrong_value_for_var(name, &raw.to_display_string())),
                };
                if n < 0 {
                    return Err(out_of_range(name, &n.to_string()));
                }
                let n = n as u128;
                if n < u128::from(*min) || n > u128::from(*max) {
                    return Err(out_of_range(name, &n.to_string()));
                }
                Ok(SysVarValue::UInt(n as u64))
            }
            DataType::Double { min, max } => {
                let f = match &raw {
                    SysVarValue::Double(d) => *d,
                    SysVarValue::Int(i) => *i as f64,
                    SysVarValue::UInt(u) => *u as f64,
                    SysVarValue::String(s) => s
                        .trim()
                        .parse::<f64>()
                        .map_err(|_| wrong_value_for_var(name, s))?,
                    _ => return Err(wrong_value_for_var(name, &raw.to_display_string())),
                };
                if !f.is_finite() || f < *min || f > *max {
                    return Err(out_of_range(name, &format_double(f)));
                }
                Ok(SysVarValue::Double(f))
            }
            DataType::Enum { values } => {
                let s = match &raw {
                    SysVarValue::String(s) => s.clone(),
                    SysVarValue::Int(_) | SysVarValue::UInt(_) => {
                        // System-variable enum indices are zero-based.
                        let idx = raw
                            .as_u64()
                            .and_then(|n| usize::try_from(n).ok())
                            .filter(|&idx| idx < values.len())
                            .ok_or_else(|| wrong_value_for_var(name, &raw.to_display_string()))?;
                        return Ok(SysVarValue::String((*values[idx]).to_string()));
                    }
                    SysVarValue::Bool(b) => {
                        if *b {
                            "ON".to_string()
                        } else {
                            "OFF".to_string()
                        }
                    }
                    _ => return Err(wrong_value_for_var(name, &raw.to_display_string())),
                };
                let canonical =
                    match_enum(values, &s).ok_or_else(|| wrong_value_for_var(name, &s))?;
                Ok(SysVarValue::String(canonical.to_string()))
            }
            DataType::SetType { values } => {
                let s = match &raw {
                    SysVarValue::String(s) => s.clone(),
                    SysVarValue::Bool(b) => {
                        if *b {
                            "ON".to_string()
                        } else {
                            "OFF".to_string()
                        }
                    }
                    SysVarValue::Int(i) => i.to_string(),
                    SysVarValue::UInt(u) => u.to_string(),
                    _ => return Err(wrong_value_for_var(name, &raw.to_display_string())),
                };
                let mut canonical: Vec<&'static str> = Vec::new();
                for piece in s.split(',') {
                    let piece = piece.trim();
                    if piece.is_empty() {
                        continue;
                    }
                    if let Some(canon) = match_enum(values, piece) {
                        if !canonical.iter().any(|v| v.eq_ignore_ascii_case(canon)) {
                            canonical.push(canon);
                        }
                    } else {
                        return Err(wrong_value_for_var(name, piece));
                    }
                }
                // Preserve a leading empty sql_mode token. MySQL's common
                // `REPLACE(@@sql_mode, 'MODE', '')` idiom can produce this
                // spelling, and clients observe the assigned string through
                // `@@sql_mode` even though mode parsing ignores empty tokens.
                let canonical = canonical.join(",");
                Ok(SysVarValue::String(if s.starts_with(',') {
                    format!(",{canonical}")
                } else {
                    canonical
                }))
            }
            DataType::String { max_len } | DataType::StrictString { max_len, .. } => {
                let s = match raw {
                    SysVarValue::String(s) => s,
                    SysVarValue::Null
                        if matches!(
                            self,
                            DataType::StrictString {
                                null_as_empty: true,
                                ..
                            }
                        ) =>
                    {
                        String::new()
                    }
                    SysVarValue::Null => return Ok(SysVarValue::Null),
                    other => other.to_display_string(),
                };
                if let Some(limit) = max_len {
                    if s.len() > *limit {
                        return Err(out_of_range(name, &s));
                    }
                }
                Ok(SysVarValue::String(s))
            }
        }
    }
}

fn match_enum(values: &[&'static str], raw: &str) -> Option<&'static str> {
    let raw_trim = raw.trim();
    values
        .iter()
        .copied()
        .find(|v| v.eq_ignore_ascii_case(raw_trim))
}

/// Compile-time definition of one system variable. Stored as a `'static`
/// in the registry; values flow through the read/write function pointers
/// attached alongside it.
#[derive(Debug, Clone)]
pub struct SysVarDef {
    pub name: &'static str,
    pub scope: Scope,
    /// Default access policy used when a scope-specific override is absent.
    pub access: Access,
    /// Optional per-scope policies for variables whose GLOBAL and SESSION
    /// surfaces differ (for example GLOBAL-writable but SESSION-read-only).
    pub session_access: Option<Access>,
    pub global_access: Option<Access>,
    /// Unsupported capability probes cannot become effective after restart either.
    pub persistable: bool,
    pub data_type: DataType,
    /// Optional list of alias names that resolve to this variable. Used
    /// for MySQL legacy aliases (e.g. `tx_isolation` → `transaction_isolation`).
    pub aliases: &'static [&'static str],
    /// Free-form documentation string shown in `information_schema.variables_info`
    /// once that view lands.
    pub doc: &'static str,
}

impl SysVarDef {
    pub const fn new(name: &'static str, scope: Scope, data_type: DataType) -> SysVarDef {
        SysVarDef {
            name,
            scope,
            access: Access::ReadWrite,
            session_access: None,
            global_access: None,
            persistable: true,
            data_type,
            aliases: &[],
            doc: "",
        }
    }

    pub const fn with_access(mut self, access: Access) -> Self {
        self.access = access;
        self
    }

    pub const fn with_scope_access(
        mut self,
        session_access: Access,
        global_access: Access,
    ) -> Self {
        self.session_access = Some(session_access);
        self.global_access = Some(global_access);
        self
    }

    pub const fn without_persistence(mut self) -> Self {
        self.persistable = false;
        self
    }

    pub const fn with_aliases(mut self, aliases: &'static [&'static str]) -> Self {
        self.aliases = aliases;
        self
    }

    pub const fn with_doc(mut self, doc: &'static str) -> Self {
        self.doc = doc;
        self
    }

    pub fn access_for(&self, scope: Scope) -> Access {
        match scope {
            Scope::Session => self.session_access.unwrap_or(self.access),
            Scope::Global => self.global_access.unwrap_or(self.access),
            Scope::Both => self.access,
        }
    }

    pub fn check_writable(&self, scope: Scope) -> Result<(), QueryError> {
        match self.access_for(scope) {
            Access::ReadWrite | Access::SuperReadWrite => Ok(()),
            Access::ReadOnly => Err(read_only_variable(self.name)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_coerce_round_trip() {
        let dt = DataType::Bool;
        let v = dt
            .coerce("foo", SysVarValue::String("ON".to_string()))
            .unwrap();
        assert_eq!(v, SysVarValue::Bool(true));
    }

    #[test]
    fn int_range_check() {
        let dt = DataType::Int { min: 0, max: 100 };
        assert!(dt.coerce("x", SysVarValue::Int(50)).is_ok());
        assert!(dt.coerce("x", SysVarValue::Int(-1)).is_err());
        assert!(dt.coerce("x", SysVarValue::Int(101)).is_err());
    }

    #[test]
    fn uint_rejects_negative_int() {
        let dt = DataType::UInt { min: 0, max: 1000 };
        assert!(dt.coerce("x", SysVarValue::Int(-5)).is_err());
        assert_eq!(
            dt.coerce("x", SysVarValue::Int(5)).unwrap(),
            SysVarValue::UInt(5)
        );
    }

    #[test]
    fn enum_canonicalises_case() {
        let dt = DataType::Enum {
            values: &["TRADITIONAL", "TREE", "JSON"],
        };
        let v = dt
            .coerce("explain_format", SysVarValue::String("json".to_string()))
            .unwrap();
        assert_eq!(v, SysVarValue::String("JSON".to_string()));
    }

    #[test]
    fn enum_accepts_numeric_index() {
        let dt = DataType::Enum {
            values: &["off", "on", "demand"],
        };
        let v = dt.coerce("event_scheduler", SysVarValue::Int(2)).unwrap();
        assert_eq!(v, SysVarValue::String("demand".to_string()));
    }

    #[test]
    fn set_type_canonicalises_and_dedupes() {
        let dt = DataType::SetType {
            values: &["hash_join", "semijoin", "loosescan"],
        };
        let v = dt
            .coerce(
                "x",
                SysVarValue::String("HASH_JOIN,semijoin,hash_join".to_string()),
            )
            .unwrap();
        assert_eq!(v, SysVarValue::String("hash_join,semijoin".to_string()));
    }

    #[test]
    fn scope_resolve_rejects_mismatches() {
        assert!(SetScope::Default
            .resolve_for("read_only", Scope::Global)
            .is_err());
        assert!(SetScope::Global
            .resolve_for("sql_mode", Scope::Session)
            .is_err());
        assert!(SetScope::Session
            .resolve_for("autocommit", Scope::Both)
            .is_ok());
        assert_eq!(
            SetScope::Default
                .resolve_for("autocommit", Scope::Both)
                .unwrap(),
            Scope::Session
        );
    }
}
