//! Database-owned REST credentials and their MySQL-only administration.
mod crypto;
use crate::{
    engine::{
        api::{QueryResponse, ResultSet, SessionContext},
        auth::{
            self, AccountKey, AuthorizationContext, Privilege, PrivilegeScope, RequiredPrivilege,
        },
    },
    parser::{ParseError, Statement},
    storage::Database,
    types::{Column, ColumnType, QueryError, Value},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

pub const MAX_KEYS_PER_ACCOUNT: usize = 1024;
pub fn error(message: impl Into<String>) -> QueryError {
    crate::error::catalog::ER_UNKNOWN_ERROR.error(message)
}
pub fn random_id() -> Result<String, QueryError> {
    let mut bytes = [0; 16];
    OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|_| error("Secure random source unavailable"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}
fn digest(secret: &[u8]) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update(b"mydb.rest.ak.digest.v1\0");
    h.update(secret);
    h.finalize().to_vec()
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub id: String,
    pub owner: AccountKey,
    pub name: String,
    pub digest: Vec<u8>,
    pub envelope: crypto::Envelope,
    pub created_at: u64,
    pub expires_at: Option<u64>,
}
impl Record {
    pub fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.id.len()
            + self.name.len()
            + self.owner.user.len()
            + self.owner.host.len()
            + self.digest.len()
            + self.envelope.ciphertext.len()
            + self.envelope.wrapped_dek.len()
            + self.envelope.master_id.len()
            + 256
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Create {
        name: String,
        owner: Option<AccountKey>,
        expires_at: Option<u64>,
    },
    List {
        owner: Option<AccountKey>,
    },
    Show {
        name: String,
        owner: Option<AccountKey>,
    },
    Drop {
        name: String,
        owner: Option<AccountKey>,
    },
    Rotate,
}
impl Command {
    pub fn is_read(&self) -> bool {
        matches!(self, Self::List { .. } | Self::Show { .. })
    }
    fn owner(&self) -> Option<&AccountKey> {
        match self {
            Self::Create { owner, .. }
            | Self::List { owner }
            | Self::Show { owner, .. }
            | Self::Drop { owner, .. } => owner.as_ref(),
            Self::Rotate => None,
        }
    }
}
struct Tokens {
    v: Vec<sqlparser::tokenizer::Token>,
    i: usize,
}
impl Tokens {
    fn eat(&mut self, word: &str) -> bool {
        if self.v.get(self.i).is_some_and(|t|matches!(t,sqlparser::tokenizer::Token::Word(w) if w.quote_style.is_none()&&w.value.eq_ignore_ascii_case(word))) {self.i+=1;true}else{false}
    }
    fn expect(&mut self, w: &str) -> Result<(), ParseError> {
        if self.eat(w) {
            Ok(())
        } else {
            Err(parse_error())
        }
    }
    fn string(&mut self) -> Result<String, ParseError> {
        let v = match self.v.get(self.i) {
            Some(sqlparser::tokenizer::Token::SingleQuotedString(v)) => v.clone(),
            Some(sqlparser::tokenizer::Token::Word(w)) => w.value.clone(),
            _ => return Err(parse_error()),
        };
        self.i += 1;
        Ok(v)
    }
    fn owner(&mut self) -> Result<Option<AccountKey>, ParseError> {
        if !self.eat("FOR") {
            return Ok(None);
        }
        let user = self.string()?;
        if self.v.get(self.i).map(ToString::to_string).as_deref() != Some("@") {
            return Err(parse_error());
        }
        self.i += 1;
        Ok(Some(AccountKey::new(user, self.string()?)))
    }
}
fn parse_error() -> ParseError {
    ParseError::at_zero("Invalid API KEY statement")
}
pub(crate) fn parse(sql: &str) -> Option<Result<Statement, ParseError>> {
    // Tokenization is restricted to possible administration statements.
    let mode =
        crate::sql_lex::SqlLexMode::from_parser_bits(crate::parser::current_parser_sql_mode_bits());
    let range =
        crate::sql_lex::first_keyword(sql, mode, crate::parser::MYSQL_VERSION_ID).ok()??;
    let head = &sql[range];
    if !["CREATE", "SHOW", "DROP", "ALTER"]
        .iter()
        .any(|w| head.eq_ignore_ascii_case(w))
    {
        return None;
    }
    if !sql
        .as_bytes()
        .windows(3)
        .any(|w| w.eq_ignore_ascii_case(b"API"))
    {
        return None;
    }
    let dialect = crate::parser::MydbDialect::default();
    let raw = sqlparser::tokenizer::Tokenizer::new(&dialect, sql)
        .tokenize()
        .ok()?;
    let mut v: Vec<_> = raw
        .into_iter()
        .filter(|t| {
            !matches!(
                t,
                sqlparser::tokenizer::Token::Whitespace(_) | sqlparser::tokenizer::Token::EOF
            )
        })
        .collect();
    if v.last() == Some(&sqlparser::tokenizer::Token::SemiColon) {
        v.pop();
    }
    let mut t = Tokens { v, i: 0 };
    let action = if t.eat("CREATE") {
        "create"
    } else if t.eat("SHOW") {
        "show"
    } else if t.eat("DROP") {
        "drop"
    } else if t.eat("ALTER") {
        "alter"
    } else {
        return None;
    };
    if action == "alter" {
        if !t.eat("INSTANCE") || !t.eat("ROTATE") || !t.eat("API") {
            return None;
        }
    } else if !t.eat("API") {
        return None;
    }
    Some((|| {
        let plural = t.eat("KEYS");
        if !plural {
            t.expect("KEY")?;
        }
        let command = if action == "alter" {
            t.expect("MASTER")?;
            t.expect("KEY")?;
            Command::Rotate
        } else if plural {
            if action != "show" {
                return Err(parse_error());
            }
            Command::List { owner: t.owner()? }
        } else {
            let name = t.string()?;
            if name.is_empty() || name.len() > 128 {
                return Err(parse_error());
            }
            let owner = t.owner()?;
            match action {
                "create" => {
                    let expires_at = if t.eat("EXPIRES") {
                        t.expect("AT")?;
                        let text = t.string()?;
                        let date =
                            chrono::NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S")
                                .map_err(|_| parse_error())?;
                        Some(
                            u64::try_from(date.and_utc().timestamp_millis())
                                .map_err(|_| parse_error())?,
                        )
                    } else {
                        None
                    };
                    Command::Create {
                        name,
                        owner,
                        expires_at,
                    }
                }
                "show" => Command::Show { name, owner },
                "drop" => Command::Drop { name, owner },
                _ => return Err(parse_error()),
            }
        };
        if t.i != t.v.len() {
            return Err(parse_error());
        }
        Ok(Statement::ApiKey(command))
    })())
}
pub(crate) fn authorize(db: &Database, s: &SessionContext, c: &Command) -> Result<(), QueryError> {
    if !s.mysql_authenticated
        || s.rest_identity.is_some()
        || s.routine_mdl_depth != 0
        || s.routine_atomic_depth != 0
        || s.user != s.authenticated_as.user
    {
        return Err(crate::error::catalog::ER_SPECIFIC_ACCESS_DENIED_ERROR
            .error("API key administration requires a top-level authenticated MySQL session"));
    }
    if matches!(
        c,
        Command::Create { .. } | Command::Show { .. } | Command::Rotate
    ) && s.ssl_cipher.is_none()
    {
        return Err(crate::error::catalog::ER_SPECIFIC_ACCESS_DENIED_ERROR
            .error("Creating or revealing API keys requires TLS"));
    }
    let owner = c.owner().unwrap_or(&s.authenticated_as);
    db.with_auth_catalog(|a| {
        if owner != &s.authenticated_as || matches!(c, Command::Rotate) {
            let required = RequiredPrivilege {
                privilege: Privilege::Dynamic("API_KEY_ADMIN".into()),
                scope: PrivilegeScope::Global,
            };
            if !a.account_has_privilege(&s.authorization_context(), &required, false) {
                return Err(auth::privilege_denied(&required.privilege, &required.scope));
            }
        }
        let account = a
            .accounts
            .get(&auth::account_id(owner))
            .ok_or_else(|| error("API key owner does not exist"))?;
        if account.is_role || owner.user.is_empty() {
            return Err(error("Roles and anonymous accounts cannot own API keys"));
        }
        if owner != &s.authenticated_as
            && a.account_is_system_user(owner)
            && !a.account_is_system_user(&s.authenticated_as)
        {
            return Err(error("SYSTEM_USER privilege required"));
        }
        Ok(())
    })?
}
fn format_time(v: u64) -> String {
    chrono::DateTime::from_timestamp_millis(v as i64)
        .map(|v| v.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default()
}
fn result(records: Vec<(Record, Option<Zeroizing<String>>)>, reveal: bool) -> QueryResponse {
    let mut names = vec![
        "key_id",
        "name",
        "owner",
        "created_at",
        "expires_at",
        "status",
    ];
    if reveal {
        names.push("api_key")
    }
    let columns = names
        .into_iter()
        .map(|n| Column {
            name: n.into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        })
        .collect();
    let rows = records
        .into_iter()
        .map(|(r, secret)| {
            let expired = r.expires_at.is_some_and(|v| v <= auth::current_time_ms());
            let mut row = vec![
                Value::String(r.id),
                Value::String(r.name),
                Value::String(r.owner.display()),
                Value::String(format_time(r.created_at)),
                r.expires_at
                    .map(|v| Value::String(format_time(v)))
                    .unwrap_or(Value::Null),
                Value::String(if expired { "expired" } else { "active" }.into()),
            ];
            if reveal {
                row.push(Value::String(
                    secret.map(|s| s.to_string()).unwrap_or_default(),
                ))
            }
            row
        })
        .collect();
    QueryResponse::ResultSet(ResultSet { columns, rows })
}
pub(crate) fn execute(
    db: &Arc<Database>,
    s: &mut SessionContext,
    c: Command,
) -> Result<QueryResponse, QueryError> {
    authorize(db, s, &c)?;
    let owner = c
        .owner()
        .cloned()
        .unwrap_or_else(|| s.authenticated_as.clone());
    // Credential DDL is intentionally local to native replication, never MySQL binlog.
    let _binlog = crate::binlog::suspend_ddl_txn_context();
    match c {
        Command::Create {
            name, expires_at, ..
        } => {
            if expires_at.is_some_and(|v| v <= auth::current_time_ms()) {
                return Err(error("API key expiration must be in the future"));
            }
            let mut created = None;
            db.mutate_auth_catalog(|a| {
                let own: Vec<_> = a.api_keys_for(&owner).collect();
                if own.len() >= MAX_KEYS_PER_ACCOUNT {
                    return Err(error("API key limit exceeded (1024 per account)"));
                }
                if own.iter().any(|r| r.name == name) {
                    return Err(error("API key name already exists"));
                }
                let keys = crypto::load(db.data_dir(), a.api_keys.is_empty())?;
                if a.api_key_master_ids().any(|id| id != &keys.active) {
                    return Err(error(
                        "Complete API key master-key rotation before creating keys",
                    ));
                }
                let id = random_id()?;
                let mut bytes = Zeroizing::new(vec![0; 32]);
                OsRng
                    .try_fill_bytes(&mut bytes)
                    .map_err(|_| error("Secure random source unavailable"))?;
                let token =
                    Zeroizing::new(format!("mydb_ak_{}_{}", id, URL_SAFE_NO_PAD.encode(&bytes)));
                let record = Record {
                    id: id.clone(),
                    owner: owner.clone(),
                    name,
                    created_at: auth::current_time_ms(),
                    expires_at,
                    digest: digest(token.as_bytes()),
                    envelope: crypto::encrypt(&keys, &id, token.as_bytes())?,
                };
                a.api_keys.insert(id, record.clone());
                created = Some((record, Some(token)));
                Ok(())
            })?;
            Ok(result(vec![created.unwrap()], true))
        }
        Command::List { .. } => Ok(result(
            db.with_auth_catalog(|a| a.api_keys_for(&owner).map(|k| (k.clone(), None)).collect())?,
            false,
        )),
        Command::Show { name, .. } => {
            let record = db
                .with_auth_catalog(|a| a.api_key_named(&owner, &name).cloned())?
                .ok_or_else(|| error("API key does not exist"))?;
            let keys = crypto::load(db.data_dir(), false)?;
            let bytes = crypto::decrypt(&keys, &record.id, &record.envelope)?;
            if !bool::from(digest(&bytes).ct_eq(&record.digest)) {
                return Err(error("API key digest mismatch"));
            }
            let token = Zeroizing::new(
                String::from_utf8(bytes.to_vec())
                    .map_err(|_| error("Invalid API key credential"))?,
            );
            Ok(result(vec![(record, Some(token))], true))
        }
        Command::Drop { name, .. } => {
            db.mutate_auth_catalog(|a| {
                let id = a
                    .api_key_named(&owner, &name)
                    .map(|k| k.id.clone())
                    .ok_or_else(|| error("API key does not exist"))?;
                a.api_keys.remove(&id);
                Ok(())
            })?;
            Ok(QueryResponse::Ok)
        }
        Command::Rotate => {
            db.mutate_auth_catalog(|a| {
                let keys = crypto::load(db.data_dir(), false)?;
                if a.api_keys
                    .values()
                    .all(|r| r.envelope.master_id == keys.active)
                {
                    return Err(error(
                        "Deploy a different API key master-key candidate before rotation",
                    ));
                }
                let records: Vec<_> = a.api_keys.values().cloned().collect();
                for mut r in records {
                    crypto::rewrap(&keys, &r.id, &mut r.envelope)?;
                    a.api_keys.insert(r.id.clone(), r);
                }
                Ok(())
            })?;
            Ok(QueryResponse::Ok)
        }
    }
}
/// Verify without charging a connection. Admission charges new sessions exactly once.
pub(crate) fn verify(
    db: &Database,
    token: &str,
    request: &auth::AuthRequest<'_>,
) -> Result<(String, auth::AuthOutcome), QueryError> {
    let denied = || {
        crate::error::catalog::ER_ACCESS_DENIED_ERROR
            .error("Invalid API key or account unavailable")
    };
    let rest = token.strip_prefix("mydb_ak_").ok_or_else(denied)?;
    if !rest.is_ascii() || rest.len() != 66 {
        return Err(denied());
    }
    let id = &rest[..22];
    if rest.as_bytes()[22] != b'_' {
        return Err(denied());
    }
    db.with_auth_catalog(|a| {
        let r = a.api_keys.get(id).ok_or_else(denied)?;
        if !bool::from(digest(token.as_bytes()).ct_eq(&r.digest))
            || r.expires_at.is_some_and(|v| v <= auth::current_time_ms())
        {
            return Err(denied());
        }
        let account = a
            .accounts
            .get(&auth::account_id(&r.owner))
            .ok_or_else(denied)?;
        if account.is_role
            || account.account_locked
            || account.password_expired
            || auth::password_lifetime_expired(
                account.password_lifetime_days,
                account.password_last_changed_ms,
            )
            || !auth::host_matches(&account.key.host, request.host)
            || auth::active_login_lock(account, auth::current_time_ms()).is_some()
        {
            return Err(denied());
        }
        crate::engine::executor::enforce_tls_requirement(&account.ssl_type, request)?;
        let context = AuthorizationContext {
            authenticated_as: account.key.clone(),
            active_roles: a.login_active_roles(&account.key).into(),
            epoch: a.epoch,
        };
        if crate::engine::sysvar::builtin::replication::offline_mode()
            && !a.account_has_privilege(
                &context,
                &RequiredPrivilege {
                    privilege: Privilege::Dynamic("CONNECTION_ADMIN".into()),
                    scope: PrivilegeScope::Global,
                },
                false,
            )
        {
            return Err(denied());
        }
        Ok((
            r.id.clone(),
            auth::AuthOutcome {
                context,
                password_expired: false,
                account_locked: false,
                resource_limits: account.resource_limits(),
                current_user_is_system: a.account_is_system_user(&r.owner),
            },
        ))
    })?
}
pub(crate) fn is_valid(db: &Database, id: &str, owner: &AccountKey) -> bool {
    let now = auth::current_time_ms();
    db.with_auth_catalog(|catalog| {
        catalog
            .api_keys
            .get(id)
            .is_some_and(|key| &key.owner == owner && key.expires_at.is_none_or(|time| time > now))
            && catalog
                .accounts
                .get(&auth::account_id(owner))
                .is_some_and(|account| {
                    !account.is_role
                        && !account.account_locked
                        && !account.password_expired
                        && auth::active_login_lock(account, now).is_none()
                        && !auth::password_lifetime_expired(
                            account.password_lifetime_days,
                            account.password_last_changed_ms,
                        )
                })
    })
    .unwrap_or(false)
}

pub(crate) fn validate_keys(db: &Database) -> Result<(), QueryError> {
    let a = db.auth_catalog();
    if a.api_keys.is_empty() {
        return Ok(());
    }
    let keys = crypto::load(db.data_dir(), false)?;
    for r in a.api_keys.values() {
        crypto::decrypt(&keys, &r.id, &r.envelope)?;
    }
    Ok(())
}
