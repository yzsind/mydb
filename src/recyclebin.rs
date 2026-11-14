//! Recycle-bin metadata and strict SQL administration grammar.
use crate::types::{QueryError, Value};
use serde::{Deserialize, Serialize};
use sqlparser::{
    dialect::MySqlDialect,
    tokenizer::{Token, Tokenizer},
};

pub const DATABASE: &str = "__mydb_recyclebin__";

thread_local! {
    static SESSION_POLICY: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    static MDL_TIMEOUT: std::cell::Cell<Option<std::time::Duration>> = const { std::cell::Cell::new(None) };
}
pub(crate) struct MdlTimeoutGuard(Option<std::time::Duration>);
impl MdlTimeoutGuard {
    pub(crate) fn enter(value: Option<u64>) -> Self {
        Self(MDL_TIMEOUT.with(|timeout| timeout.replace(value.map(std::time::Duration::from_secs))))
    }
}
impl Drop for MdlTimeoutGuard {
    fn drop(&mut self) {
        MDL_TIMEOUT.with(|timeout| timeout.set(self.0));
    }
}
pub(crate) fn mdl_timeout(fallback: std::time::Duration) -> std::time::Duration {
    MDL_TIMEOUT.with(|timeout| timeout.get().unwrap_or(fallback))
}
pub(crate) struct PolicyGuard(Option<bool>);
impl PolicyGuard {
    pub(crate) fn enter(value: Option<bool>) -> Self {
        Self(SESSION_POLICY.with(|p| p.replace(value)))
    }
}
impl Drop for PolicyGuard {
    fn drop(&mut self) {
        SESSION_POLICY.with(|p| p.set(self.0));
    }
}
pub(crate) fn session_policy() -> Option<bool> {
    SESSION_POLICY.with(|p| p.get())
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Config {
    pub enabled: bool,
    pub retention_seconds: u64,
    pub min_free_pct: u64,
    pub target_free_pct: u64,
    pub gc_interval_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Show {
        database: Option<String>,
        pattern: Option<String>,
    },
    Restore {
        name: String,
        target: Option<Destination>,
    },
    Purge {
        name: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Destination {
    pub database: Option<String>,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub id: u64,
    pub table_id: u64,
    pub origin_database: String,
    pub origin_table: String,
    pub operation: String,
    pub operation_id: u64,
    pub recycled_ms: u64,
    pub expires_ms: u64,
    pub purging: bool,
    pub omitted: Vec<String>,
    pub check_names: Vec<String>,
    pub data_bytes: u64,
    pub manifest: String,
    pub last_error: String,
    pub purge_reason: String,
}

impl Entry {
    pub fn name(&self) -> String {
        format!("r_{}", self.id)
    }
    pub(crate) fn row(&self) -> Result<Vec<Value>, QueryError> {
        Ok(vec![
            Value::UInt(self.id),
            Value::UInt(if self.purging { 0 } else { self.expires_ms }),
            Value::String(serde_json::to_string(self).map_err(|e| failure(e.to_string()))?),
        ])
    }
    pub(crate) fn decode(row: &[Value]) -> Result<Self, QueryError> {
        let Some(Value::String(json)) = row.get(2) else {
            return Err(failure("invalid recycle-bin record"));
        };
        let entry: Self = serde_json::from_str(json).map_err(|e| failure(e.to_string()))?;
        if row.first() != Some(&Value::UInt(entry.id))
            || row.get(1)
                != Some(&Value::UInt(if entry.purging {
                    0
                } else {
                    entry.expires_ms
                }))
        {
            return Err(failure("recycle-bin identity mismatch"));
        }
        Ok(entry)
    }
}

pub(crate) fn failure(message: impl Into<String>) -> QueryError {
    crate::storage::error(crate::error::catalog::MYDB_ER_RECYCLEBIN, message)
}

pub(crate) fn require_current_admin(db: &crate::storage::Database) -> Result<(), QueryError> {
    let Some((context, internal)) = crate::engine::expr::current_authorization_context() else {
        return Ok(());
    };
    if internal
        || db.with_auth_catalog(|auth| {
            auth.account_has_privilege(
                &context,
                &crate::engine::auth::RequiredPrivilege {
                    privilege: crate::engine::auth::Privilege::Dynamic("RECYCLEBIN_ADMIN".into()),
                    scope: crate::engine::auth::PrivilegeScope::Global,
                },
                false,
            )
        })?
    {
        return Ok(());
    }
    Err(crate::storage::error(
        crate::error::catalog::ER_SPECIFIC_ACCESS_DENIED_ERROR,
        "RECYCLEBIN_ADMIN is required",
    ))
}

pub(crate) fn validate_query_access(
    statement: &crate::parser::Statement,
    database: &str,
) -> Result<(), QueryError> {
    struct Check<'a> {
        database: &'a str,
        recycled: bool,
        forbidden: bool,
    }
    impl crate::parser::visit::AstVisitor for Check<'_> {
        fn enter_select(&mut self, select: &crate::parser::Select) {
            let mut recycled = false;
            for source in select
                .from
                .iter()
                .chain(select.joins.iter().map(|j| &j.table))
            {
                if crate::storage::split_table_name(self.database, &source.name).0 == DATABASE {
                    self.recycled = true;
                    recycled = true;
                    self.forbidden |= source.system_time.is_some();
                }
            }
            self.forbidden |= recycled && (select.for_update || select.lock_clause.is_some());
        }
    }
    let mut check = Check {
        database,
        recycled: false,
        forbidden: false,
    };
    crate::parser::visit::visit_statement(&mut check, statement);
    if check.forbidden
        || (check.recycled
            && matches!(
                statement,
                crate::parser::Statement::CreateView { .. }
                    | crate::parser::Statement::AlterView { .. }
            ))
    {
        Err(failure("recycle-bin tables do not support locking reads, time travel, or persistent view dependencies"))
    } else {
        Ok(())
    }
}

pub(crate) const COLUMNS: &[&str] = &[
    "RECYCLE_ID",
    "RECYCLE_NAME",
    "ORIGIN_SCHEMA",
    "ORIGIN_TABLE",
    "OPERATION",
    "RECYCLED_MS",
    "EXPIRES_MS",
    "STATE",
    "OMITTED_OBJECTS",
    "LAST_ERROR",
    "OPERATION_ID",
    "DATA_BYTES",
    "PURGE_REASON",
];
pub(crate) fn metadata_row(e: &Entry) -> Vec<Value> {
    vec![
        Value::UInt(e.id),
        Value::String(e.name()),
        Value::String(e.origin_database.clone()),
        Value::String(e.origin_table.clone()),
        Value::String(e.operation.clone()),
        Value::UInt(e.recycled_ms),
        Value::UInt(e.expires_ms),
        Value::String(if e.purging { "PURGING" } else { "RECYCLED" }.into()),
        Value::String(e.omitted.join(", ")),
        Value::String(e.last_error.clone()),
        Value::UInt(e.operation_id),
        Value::UInt(e.data_bytes),
        Value::String(e.purge_reason.clone()),
    ]
}
pub(crate) fn result(rows: Vec<Vec<Value>>) -> crate::engine::QueryResponse {
    crate::engine::QueryResponse::ResultSet(crate::engine::ResultSet {
        columns: COLUMNS
            .iter()
            .enumerate()
            .map(|(i, name)| crate::types::Column {
                name: (*name).into(),
                column_type: if [0, 5, 6, 10, 11].contains(&i) {
                    crate::types::ColumnType::LongLong
                } else {
                    crate::types::ColumnType::VarString
                },
                source: Default::default(),
            })
            .collect(),
        rows,
    })
}

pub(crate) fn like(value: &str, pattern: &str) -> bool {
    let collation = crate::charset::resolve_collation("utf8mb4_general_ci")
        .expect("builtin metadata collation");
    crate::engine::expr::match_like_pattern_with_info(value, pattern, Some('\\'), collation)
}

pub(crate) fn parse(sql: &str) -> Result<Option<Command>, QueryError> {
    let normalized = crate::sql_lex::normalize_admin_sql(sql);
    if !["SHOW RECYCLEBIN", "RESTORE TABLE", "PURGE RECYCLEBIN"]
        .iter()
        .any(|p| normalized == *p || normalized.starts_with(&format!("{p} ")))
    {
        return Ok(None);
    }
    let tokens = Tokenizer::new(&MySqlDialect {}, sql)
        .tokenize()
        .map_err(|e| failure(e.to_string()))?;
    let mut tokens: Vec<Token> = tokens
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect();
    if tokens.last() == Some(&Token::SemiColon) {
        tokens.pop();
    }
    let mut t = Tokens { tokens, offset: 0 };
    let command = if t.word("SHOW") {
        t.expect("RECYCLEBIN")?;
        let database = if t.word("FROM") || t.word("IN") {
            Some(t.identifier()?)
        } else {
            None
        };
        let pattern = if t.word("LIKE") {
            match t.tokens.get(t.offset).cloned() {
                Some(Token::SingleQuotedString(s)) => {
                    t.offset += 1;
                    Some(s)
                }
                _ => return Err(failure("RECYCLEBIN LIKE requires a string")),
            }
        } else {
            None
        };
        Command::Show { database, pattern }
    } else if t.word("RESTORE") {
        t.expect("TABLE")?;
        let name = t.identifier()?;
        let target = if t.word("TO") {
            let first = t.identifier()?;
            if t.tokens.get(t.offset) == Some(&Token::Period) {
                t.offset += 1;
                Some(Destination {
                    database: Some(first),
                    table: t.identifier()?,
                })
            } else {
                Some(Destination {
                    database: None,
                    table: first,
                })
            }
        } else {
            None
        };
        Command::Restore { name, target }
    } else {
        t.expect("PURGE")?;
        t.expect("RECYCLEBIN")?;
        Command::Purge {
            name: if t.word("ALL") {
                None
            } else {
                Some(t.identifier()?)
            },
        }
    };
    if t.offset != t.tokens.len() {
        return Err(failure("unexpected trailing recycle-bin SQL"));
    }
    Ok(Some(command))
}

struct Tokens {
    tokens: Vec<Token>,
    offset: usize,
}
impl Tokens {
    fn word(&mut self, expected: &str) -> bool {
        if matches!(self.tokens.get(self.offset), Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(expected))
        {
            self.offset += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, expected: &str) -> Result<(), QueryError> {
        if self.word(expected) {
            Ok(())
        } else {
            Err(failure(format!("expected {expected} in recycle-bin SQL")))
        }
    }
    fn identifier(&mut self) -> Result<String, QueryError> {
        match self.tokens.get(self.offset).cloned() {
            Some(Token::Word(w)) => {
                if w.value.contains('.') {
                    return Err(failure(
                        "Dots inside quoted object identifiers are not supported",
                    ));
                }
                self.offset += 1;
                Ok(w.value)
            }
            _ => Err(failure("expected identifier in recycle-bin SQL")),
        }
    }
}
