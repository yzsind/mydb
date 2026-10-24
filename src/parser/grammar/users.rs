//! CREATE/ALTER USER share typed authentication and account-policy grammar.
use super::super::*;
use super::cursor::Cursor;

fn number<T: std::str::FromStr>(cursor: &mut Cursor<'_>) -> Result<T, ParseError> {
    let value = cursor.account_text()?;
    value
        .parse()
        .map_err(|_| cursor.expected("nonnegative integer"))
}

fn policy_number(cursor: &mut Cursor<'_>) -> Result<AccountPolicy<u16>, ParseError> {
    if cursor.keyword(b"DEFAULT")? {
        Ok(AccountPolicy::Default)
    } else {
        number(cursor).map(AccountPolicy::Value)
    }
}

fn ssl_requirement(cursor: &mut Cursor<'_>) -> Result<UserSslRequirement, ParseError> {
    if cursor.keyword(b"NONE")? {
        return Ok(UserSslRequirement::None);
    }
    if cursor.keyword(b"SSL")? {
        return Ok(UserSslRequirement::Ssl);
    }
    if cursor.keyword(b"X509")? {
        return Ok(UserSslRequirement::X509);
    }
    let (mut cipher, mut issuer, mut subject) = (None, None, None);
    loop {
        if cursor.keyword(b"CIPHER")? {
            cipher = Some(cursor.account_text()?);
        } else if cursor.keyword(b"ISSUER")? {
            issuer = Some(cursor.account_text()?);
        } else if cursor.keyword(b"SUBJECT")? {
            subject = Some(cursor.account_text()?);
        } else {
            break;
        }
        let _ = cursor.keyword(b"AND")?;
    }
    if cipher.is_none() && issuer.is_none() && subject.is_none() {
        return Err(cursor.expected("NONE, SSL, X509, CIPHER, ISSUER or SUBJECT"));
    }
    Ok(UserSslRequirement::Specified {
        cipher,
        issuer,
        subject,
    })
}

fn resource_options(
    cursor: &mut Cursor<'_>,
    out: &mut ResourceOptionsPatch,
) -> Result<(), ParseError> {
    loop {
        if cursor.keyword(b"MAX_QUERIES_PER_HOUR")? {
            out.max_queries_per_hour = Some(number(cursor)?);
        } else if cursor.keyword(b"MAX_UPDATES_PER_HOUR")? {
            out.max_updates_per_hour = Some(number(cursor)?);
        } else if cursor.keyword(b"MAX_CONNECTIONS_PER_HOUR")? {
            out.max_connections_per_hour = Some(number(cursor)?);
        } else if cursor.keyword(b"MAX_USER_CONNECTIONS")? {
            out.max_user_connections = Some(number(cursor)?);
        } else {
            return Ok(());
        }
    }
}

fn empty_user(account: AccountIdent, current_user: bool, if_exists: bool) -> AlterUser {
    AlterUser {
        account,
        current_user,
        if_exists,
        password: None,
        plugin: None,
        password_is_pre_hashed: false,
        replace_old_password: None,
        random_password: false,
        retain_current_password: false,
        discard_old_password: false,
        account_locked: None,
        password_expired: None,
        ssl_requirement: None,
        resource_options: ResourceOptionsPatch::default(),
        failed_login_attempts: None,
        password_lock_time_days: None,
        password_lifetime_days: None,
        password_history: None,
        password_reuse_interval_days: None,
        password_require_current: None,
        comment: None,
        attribute_json: None,
    }
}

// Explicit masks preserve CREATE's last-account statement policy. DEFAULT is
// an explicit change even when its resulting CREATE field is None.
const LOCK: u16 = 1 << 0;
const EXPIRE: u16 = 1 << 1;
const SSL: u16 = 1 << 2;
const RESOURCE: u16 = 1 << 3;
const FAILED: u16 = 1 << 4;
const LOCK_TIME: u16 = 1 << 5;
const LIFETIME: u16 = 1 << 6;
const HISTORY: u16 = 1 << 7;
const REUSE: u16 = 1 << 8;
const CURRENT: u16 = 1 << 9;
const COMMENT: u16 = 1 << 10;
const ATTRIBUTE: u16 = 1 << 11;

fn copy_policy(out: &mut AlterUser, source: &AlterUser, mask: u16) {
    macro_rules! copy {
        ($($flag:ident => $field:ident),+ $(,)?) => { $(
            if mask & $flag != 0 { out.$field = source.$field.clone(); }
        )+ };
    }
    copy!(LOCK => account_locked, EXPIRE => password_expired, SSL => ssl_requirement,
        RESOURCE => resource_options, FAILED => failed_login_attempts,
        LOCK_TIME => password_lock_time_days, LIFETIME => password_lifetime_days,
        HISTORY => password_history, REUSE => password_reuse_interval_days,
        CURRENT => password_require_current, COMMENT => comment, ATTRIBUTE => attribute_json);
}

fn options(
    cursor: &mut Cursor<'_>,
    out: &mut AlterUser,
    create: bool,
    default_roles: &mut Vec<AccountIdent>,
) -> Result<u16, ParseError> {
    let mut mask = 0;
    loop {
        if cursor.keyword(b"IDENTIFIED")? {
            if cursor.keyword(b"WITH")? {
                out.plugin = Some(cursor.account_text()?);
                if cursor.keyword(b"AS")? {
                    out.password = Some(cursor.account_text()?);
                    out.password_is_pre_hashed = true;
                    continue;
                }
                if !cursor.keyword(b"BY")? {
                    continue;
                }
            } else {
                cursor.expect_keyword(b"BY")?;
            }
            if cursor.keyword(b"RANDOM")? {
                cursor.expect_keyword(b"PASSWORD")?;
                out.random_password = true;
            } else {
                out.password = Some(cursor.account_text()?);
            }
        } else if !create && cursor.keyword(b"REPLACE")? {
            out.replace_old_password = Some(cursor.account_text()?);
        } else if !create && cursor.keyword(b"RETAIN")? {
            cursor.expect_keyword(b"CURRENT")?;
            cursor.expect_keyword(b"PASSWORD")?;
            out.retain_current_password = true;
        } else if !create && cursor.keyword(b"DISCARD")? {
            cursor.expect_keyword(b"OLD")?;
            cursor.expect_keyword(b"PASSWORD")?;
            out.discard_old_password = true;
        } else if cursor.keyword(b"ACCOUNT")? {
            out.account_locked = Some(if cursor.keyword(b"LOCK")? {
                true
            } else {
                cursor.expect_keyword(b"UNLOCK")?;
                false
            });
            mask |= LOCK;
        } else if cursor.keyword(b"PASSWORD")? {
            if cursor.keyword(b"EXPIRE")? {
                mask |= EXPIRE | LIFETIME;
                if cursor.keyword(b"INTERVAL")? {
                    out.password_lifetime_days = Some(AccountPolicy::Value(number(cursor)?));
                    cursor.expect_keyword(b"DAY")?;
                } else if cursor.keyword(b"NEVER")? {
                    out.password_lifetime_days = Some(AccountPolicy::Value(0));
                } else if cursor.keyword(b"DEFAULT")? {
                    out.password_lifetime_days = Some(AccountPolicy::Default);
                } else {
                    out.password_expired = Some(true);
                }
            } else if cursor.keyword(b"HISTORY")? {
                out.password_history = Some(policy_number(cursor)?);
                mask |= HISTORY;
            } else if cursor.keyword(b"REUSE")? {
                cursor.expect_keyword(b"INTERVAL")?;
                let policy = policy_number(cursor)?;
                if matches!(policy, AccountPolicy::Value(_)) {
                    cursor.expect_keyword(b"DAY")?;
                }
                out.password_reuse_interval_days = Some(policy);
                mask |= REUSE;
            } else if cursor.keyword(b"REQUIRE")? {
                cursor.expect_keyword(b"CURRENT")?;
                out.password_require_current = Some(if cursor.keyword(b"OPTIONAL")? {
                    AccountPolicy::Value(false)
                } else if cursor.keyword(b"DEFAULT")? {
                    AccountPolicy::Default
                } else {
                    AccountPolicy::Value(true)
                });
                mask |= CURRENT;
            } else if cursor.keyword(b"LIFETIME")? {
                out.password_lifetime_days = Some(AccountPolicy::Value(number(cursor)?));
                mask |= LIFETIME;
            } else {
                return Err(cursor.expected("password policy"));
            }
        } else if cursor.keyword(b"FAILED_LOGIN_ATTEMPTS")? {
            out.failed_login_attempts = Some(number(cursor)?);
            mask |= FAILED;
        } else if cursor.keyword(b"PASSWORD_LOCK_TIME")? {
            out.password_lock_time_days = Some(if cursor.keyword(b"UNBOUNDED")? {
                u16::MAX
            } else {
                number(cursor)?
            });
            mask |= LOCK_TIME;
        } else if cursor.keyword(b"REQUIRE")? {
            out.ssl_requirement = Some(ssl_requirement(cursor)?);
            mask |= SSL;
        } else if cursor.keyword(b"WITH")? {
            resource_options(cursor, &mut out.resource_options)?;
            mask |= RESOURCE;
        } else if cursor.keyword(b"ATTRIBUTE")? {
            out.attribute_json = Some(cursor.account_text()?);
            mask |= ATTRIBUTE;
        } else if cursor.keyword(b"COMMENT")? {
            out.comment = Some(cursor.account_text()?);
            mask |= COMMENT;
        } else if create && cursor.keyword(b"DEFAULT")? {
            cursor.expect_keyword(b"ROLE")?;
            default_roles.extend(cursor.accounts()?);
        } else {
            return Ok(mask);
        }
    }
}

fn policy_value<T>(value: Option<AccountPolicy<T>>) -> Option<T> {
    match value {
        Some(AccountPolicy::Value(v)) => Some(v),
        _ => None,
    }
}

fn into_create(user: AlterUser) -> UserSpec {
    UserSpec {
        account: user.account,
        password: user.password,
        random_password: user.random_password,
        plugin: user.plugin,
        password_is_pre_hashed: user.password_is_pre_hashed,
        account_locked: user.account_locked.unwrap_or(false),
        password_expired: user.password_expired.unwrap_or(false),
        ssl_requirement: user.ssl_requirement,
        resource_options: ResourceOptions {
            max_queries_per_hour: user.resource_options.max_queries_per_hour.unwrap_or(0),
            max_updates_per_hour: user.resource_options.max_updates_per_hour.unwrap_or(0),
            max_connections_per_hour: user.resource_options.max_connections_per_hour.unwrap_or(0),
            max_user_connections: user.resource_options.max_user_connections.unwrap_or(0),
        },
        failed_login_attempts: user.failed_login_attempts,
        password_lock_time_days: user.password_lock_time_days,
        password_lifetime_days: policy_value(user.password_lifetime_days),
        password_history: policy_value(user.password_history),
        password_reuse_interval_days: policy_value(user.password_reuse_interval_days),
        password_require_current: policy_value(user.password_require_current),
        comment: user.comment,
        attribute_json: user.attribute_json,
    }
}

pub(in crate::parser) fn parse_user_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let mut cursor = Cursor::new(sql);
    let create = if cursor.keyword(b"CREATE")? {
        true
    } else if cursor.keyword(b"ALTER")? {
        false
    } else {
        return Ok(None);
    };
    if !cursor.keyword(b"USER")? {
        return Ok(None);
    }
    let optional = if cursor.keyword(b"IF")? {
        if create {
            cursor.expect_keyword(b"NOT")?;
        }
        cursor.expect_keyword(b"EXISTS")?;
        true
    } else {
        false
    };
    let mut users = Vec::new();
    let mut masks = Vec::new();
    let mut default_roles = Vec::new();
    loop {
        let current_user = !create && cursor.keyword(b"CURRENT_USER")?;
        let account = if current_user {
            if cursor.symbol(b'(')? {
                cursor.expect_symbol(b')')?;
            }
            AccountIdent {
                user: String::new(),
                host: String::new(),
            }
        } else {
            cursor.account()?
        };
        let mut user = empty_user(account, current_user, optional);
        masks.push(options(&mut cursor, &mut user, create, &mut default_roles)?);
        users.push(user);
        if !cursor.symbol(b',')? {
            break;
        }
    }
    cursor.finish()?;
    if users.len() > 1 {
        let last = users.len() - 1;
        if !create && masks[..last].iter().any(|&mask| mask != 0) {
            return Err(cursor.expected("statement-wide policies after the final account"));
        }
        let policy = users[last].clone();
        for user in &mut users[..last] {
            copy_policy(user, &policy, masks[last]);
        }
    }
    Ok(Some(if create {
        Statement::CreateUser(CreateUser {
            accounts: users.into_iter().map(into_create).collect(),
            if_not_exists: optional,
            is_role: false,
            default_roles,
        })
    } else if users.len() == 1 {
        Statement::AlterUser(users.pop().unwrap())
    } else {
        Statement::AlterUsers(users)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authentication_literals_and_required_values_keep_their_identity() {
        let Some(Statement::CreateUser(user)) =
            parse_user_statement("CREATE USER u IDENTIFIED BY 'RANDOM' PASSWORD EXPIRE").unwrap()
        else {
            panic!("CREATE USER");
        };
        assert_eq!(user.accounts[0].password.as_deref(), Some("RANDOM"));
        assert!(!user.accounts[0].random_password);
        assert!(user.accounts[0].password_expired);
        for sql in [
            "CREATE USER u IDENTIFIED BY",
            "ALTER USER u IDENTIFIED BY",
            "CREATE USER u IDENTIFIED WITH p AS",
            "ALTER USER u IDENTIFIED WITH p AS",
            "CREATE USER u,",
        ] {
            assert!(parse_user_statement(sql).is_err(), "{sql}");
        }
    }
}
