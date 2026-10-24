//! GRANT/REVOKE preserve quote identity through the common grammar cursor.
use super::super::*;
use super::cursor::Cursor;
use super::roles::role_choice;
use crate::sql_lex::SqlTokenKind;

pub(super) fn account(cursor: &mut Cursor<'_>) -> Result<AccountIdent, ParseError> {
    if let Some(token) = cursor.peek()?.filter(|token| {
        token.kind == SqlTokenKind::Word && cursor.text(token).eq_ignore_ascii_case("CURRENT_USER")
    }) {
        cursor.next()?;
        if cursor.symbol(b'(')? {
            cursor.expect_symbol(b')')?;
            return Ok(AccountIdent {
                user: "CURRENT_USER()".into(),
                host: String::new(),
            });
        }
        let host = if cursor.symbol(b'@')? {
            cursor.account_text()?
        } else {
            "%".into()
        };
        return Ok(AccountIdent {
            user: cursor.text(&token).to_owned(),
            host,
        });
    }
    cursor.account()
}
fn accounts(cursor: &mut Cursor<'_>) -> Result<Vec<AccountIdent>, ParseError> {
    let mut result = vec![account(cursor)?];
    while cursor.symbol(b',')? {
        result.push(account(cursor)?);
    }
    Ok(result)
}

fn header_has_on(cursor: &Cursor<'_>, end: &str) -> Result<bool, ParseError> {
    let mut lookahead = Cursor::new(&cursor.source()[cursor.position()..]);
    let mut depth = 0usize;
    while let Some(token) = lookahead.next()? {
        match token.kind {
            SqlTokenKind::Symbol(b'(') => depth += 1,
            SqlTokenKind::Symbol(b')') => depth = depth.saturating_sub(1),
            SqlTokenKind::Word if depth == 0 => {
                if lookahead.text(&token).eq_ignore_ascii_case("ON") {
                    return Ok(true);
                }
                if lookahead.text(&token).eq_ignore_ascii_case(end) {
                    return Ok(false);
                }
            }
            _ => {}
        }
    }
    Err(lookahead.expected(end))
}

fn starts_keywords(cursor: &Cursor<'_>, words: &[&[u8]]) -> Result<bool, ParseError> {
    let mut lookahead = Cursor::new(&cursor.source()[cursor.position()..]);
    for word in words {
        if !lookahead.keyword(word)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn privilege_specs(cursor: &mut Cursor<'_>) -> Result<Vec<PrivilegeSpec>, ParseError> {
    let mut result = Vec::new();
    loop {
        let mut words = Vec::new();
        while let Some(token) = cursor.peek()? {
            if token.kind != SqlTokenKind::Word || cursor.text(&token).eq_ignore_ascii_case("ON") {
                break;
            }
            words.push(cursor.identifier()?);
        }
        if words.is_empty() {
            return Err(cursor.expected("privilege name"));
        }
        let mut columns = Vec::new();
        if cursor.symbol(b'(')? {
            loop {
                columns.push(cursor.identifier()?.to_ascii_lowercase());
                if !cursor.symbol(b',')? {
                    break;
                }
            }
            cursor.expect_symbol(b')')?;
        }
        result.push(PrivilegeSpec {
            name: words.join(" "),
            columns,
        });
        if !cursor.symbol(b',')? {
            break;
        }
    }
    Ok(result)
}

fn scope(cursor: &mut Cursor<'_>) -> Result<GrantScope, ParseError> {
    let routine = if cursor.keyword(b"PROCEDURE")? {
        Some(RoutineKind::Procedure)
    } else if cursor.keyword(b"FUNCTION")? {
        Some(RoutineKind::Function)
    } else {
        let _ = cursor.keyword(b"TABLE")?;
        None
    };
    let first_wildcard = cursor.symbol(b'*')?;
    let first = if first_wildcard {
        "*".into()
    } else {
        cursor.identifier()?
    };
    let scope = if cursor.symbol(b'.')? {
        let second_wildcard = cursor.symbol(b'*')?;
        let second = if second_wildcard {
            "*".into()
        } else {
            cursor.identifier()?
        };
        match (first_wildcard, second_wildcard) {
            (true, true) => GrantScope::Global,
            (true, false) => return Err(cursor.expected("* after * dot")),
            (false, true) => GrantScope::Database(first),
            (false, false) => GrantScope::Table {
                database: Some(first),
                table: second,
            },
        }
    } else if first_wildcard {
        GrantScope::Database(String::new())
    } else {
        GrantScope::Table {
            database: None,
            table: first,
        }
    };
    if let Some(kind) = routine {
        match scope {
            GrantScope::Table { database, table } => Ok(GrantScope::Routine {
                database,
                name: table,
                kind,
            }),
            _ => Err(cursor.expected("routine name")),
        }
    } else {
        Ok(scope)
    }
}

fn grant(cursor: &mut Cursor<'_>) -> Result<Statement, ParseError> {
    let kind = if cursor.keyword(b"PROXY")? {
        cursor.expect_keyword(b"ON")?;
        GrantKind::Proxy(account(cursor)?)
    } else if header_has_on(cursor, "TO")? {
        let privileges = privilege_specs(cursor)?;
        cursor.expect_keyword(b"ON")?;
        GrantKind::Privileges {
            privileges,
            scope: scope(cursor)?,
        }
    } else {
        GrantKind::Roles(accounts(cursor)?)
    };
    cursor.expect_keyword(b"TO")?;
    let targets = accounts(cursor)?;
    let mut grant_option = false;
    let mut grant_as = None;
    loop {
        if cursor.keyword(b"WITH")? {
            if cursor.keyword(b"GRANT")? {
                cursor.expect_keyword(b"OPTION")?;
            } else if matches!(kind, GrantKind::Roles(_)) && cursor.keyword(b"ADMIN")? {
                cursor.expect_keyword(b"OPTION")?;
            } else {
                return Err(cursor.expected("GRANT OPTION or ADMIN OPTION"));
            }
            grant_option = true;
        } else if matches!(kind, GrantKind::Privileges { .. }) && cursor.keyword(b"REQUIRE")? {
            // TLS restrictions are supported on CREATE/ALTER USER, not GRANT.
            // NONE is the only grant tail that imposes no account restriction.
            cursor.expect_keyword(b"NONE")?;
        } else if matches!(kind, GrantKind::Privileges { .. }) && cursor.keyword(b"AS")? {
            let account = account(cursor)?;
            let roles = if cursor.keyword(b"WITH")? {
                cursor.expect_keyword(b"ROLE")?;
                Some(role_choice(cursor)?)
            } else {
                None
            };
            grant_as = Some(GrantAs { account, roles });
            break;
        } else {
            break;
        }
    }
    cursor.finish()?;
    Ok(Statement::Grant(Grant {
        kind,
        targets,
        grant_option,
        grant_as,
    }))
}

fn revoke(cursor: &mut Cursor<'_>) -> Result<Statement, ParseError> {
    let if_exists = if cursor.keyword(b"IF")? {
        cursor.expect_keyword(b"EXISTS")?;
        true
    } else {
        false
    };
    let mut revoke_all_from_target = false;
    let mut grant_option_only = false;
    let kind = if cursor.keyword(b"PROXY")? {
        cursor.expect_keyword(b"ON")?;
        GrantKind::Proxy(account(cursor)?)
    } else if header_has_on(cursor, "FROM")? {
        grant_option_only = starts_keywords(cursor, &[b"GRANT", b"OPTION", b"FOR"])?;
        if grant_option_only {
            cursor.expect_keyword(b"GRANT")?;
            cursor.expect_keyword(b"OPTION")?;
            cursor.expect_keyword(b"FOR")?;
        }
        let privileges = privilege_specs(cursor)?;
        cursor.expect_keyword(b"ON")?;
        GrantKind::Privileges {
            privileges,
            scope: scope(cursor)?,
        }
    } else if cursor.keyword(b"ALL")? {
        let _ = cursor.keyword(b"PRIVILEGES")?;
        let comma = cursor.symbol(b',')?;
        if comma || cursor.keyword(b"GRANT")? {
            if comma {
                cursor.expect_keyword(b"GRANT")?;
            }
            cursor.expect_keyword(b"OPTION")?;
        }
        revoke_all_from_target = true;
        GrantKind::Privileges {
            privileges: Vec::new(),
            scope: GrantScope::Global,
        }
    } else {
        GrantKind::Roles(accounts(cursor)?)
    };
    cursor.expect_keyword(b"FROM")?;
    let targets = accounts(cursor)?;
    let ignore_unknown_user = if cursor.keyword(b"IGNORE")? {
        cursor.expect_keyword(b"UNKNOWN")?;
        cursor.expect_keyword(b"USER")?;
        true
    } else {
        false
    };
    cursor.finish()?;
    Ok(Statement::Revoke(Revoke {
        kind,
        targets,
        revoke_all_from_target,
        grant_option_only,
        if_exists,
        ignore_unknown_user,
    }))
}

pub(in crate::parser) fn parse_privilege_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let mut cursor = Cursor::new(sql);
    if cursor.keyword(b"GRANT")? {
        grant(&mut cursor).map(Some)
    } else if cursor.keyword(b"REVOKE")? {
        revoke(&mut cursor).map(Some)
    } else {
        Ok(None)
    }
}

pub(in crate::parser) fn parse_definer(sql: &str) -> Result<AccountIdent, ParseError> {
    let mut cursor = Cursor::new(sql);
    let result = account(&mut cursor)?;
    cursor.finish()?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoted_keywords_remain_accounts_and_roles() {
        let Some(Statement::Grant(grant)) =
            parse_privilege_statement("GRANT SELECT ON test.t TO 'AS'@'WITH'").unwrap()
        else {
            panic!("GRANT");
        };
        assert_eq!(
            grant.targets,
            [AccountIdent {
                user: "AS".into(),
                host: "WITH".into()
            }]
        );
        assert!(grant.grant_as.is_none());
        let Some(Statement::Revoke(revoke)) =
            parse_privilege_statement("REVOKE 'ALL' FROM current_user").unwrap()
        else {
            panic!("REVOKE");
        };
        assert!(!revoke.revoke_all_from_target);
        assert!(matches!(revoke.kind, GrantKind::Roles(_)));
        assert_eq!(revoke.targets[0].user, "current_user");
        let Some(Statement::Grant(grant)) =
            parse_privilege_statement("GRANT 'PROXY' TO u").unwrap()
        else {
            panic!("role GRANT");
        };
        assert!(matches!(grant.kind, GrantKind::Roles(_)));
    }

    #[test]
    fn grant_tails_cannot_silently_discard_account_or_role_options() {
        for sql in [
            "GRANT SELECT ON test.t TO u WITH MAX_USER_CONNECTIONS 1",
            "GRANT SELECT ON test.t TO u AS grantor WITH ROLE ALL garbage",
            "GRANT SELECT ON test.t TO u WITH ADMIN OPTION",
        ] {
            assert!(parse_privilege_statement(sql).is_err(), "{sql}");
        }
        let Some(Statement::Revoke(revoke)) =
            parse_privilege_statement("REVOKE GRANT OPTION ON test.* FROM u").unwrap()
        else {
            panic!("native privilege");
        };
        assert!(!revoke.grant_option_only);
        let Some(Statement::Revoke(revoke)) =
            parse_privilege_statement("REVOKE GRANT OPTION FOR SELECT ON test.* FROM u").unwrap()
        else {
            panic!("grant-option patch");
        };
        assert!(revoke.grant_option_only);
    }
}
