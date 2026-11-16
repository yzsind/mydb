//! Account statements that do not need privilege or authentication-option ASTs.
use super::super::{AccountIdent, ParseError, Statement};
use super::cursor::Cursor;

fn current_or_account(cursor: &mut Cursor<'_>) -> Result<AccountIdent, ParseError> {
    if cursor.keyword(b"CURRENT_USER")? {
        if cursor.symbol(b'(')? {
            cursor.expect_symbol(b')')?;
        }
        Ok(AccountIdent {
            user: "CURRENT_USER()".into(),
            host: String::new(),
        })
    } else {
        cursor.account()
    }
}

pub(in crate::parser) fn parse_account_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let mut cursor = Cursor::new(sql);
    let statement = if cursor.keyword(b"CREATE")? {
        if !cursor.keyword(b"ROLE")? {
            return Ok(None);
        }
        let if_not_exists = if cursor.keyword(b"IF")? {
            cursor.expect_keyword(b"NOT")?;
            cursor.expect_keyword(b"EXISTS")?;
            true
        } else {
            false
        };
        Statement::CreateRole {
            roles: cursor.accounts()?,
            if_not_exists,
        }
    } else if cursor.keyword(b"DROP")? {
        let user = cursor.keyword(b"USER")?;
        if !user && !cursor.keyword(b"ROLE")? {
            return Ok(None);
        }
        let if_exists = if cursor.keyword(b"IF")? {
            cursor.expect_keyword(b"EXISTS")?;
            true
        } else {
            false
        };
        let accounts = cursor.accounts()?;
        if user {
            Statement::DropUser {
                accounts,
                if_exists,
            }
        } else {
            Statement::DropRole {
                roles: accounts,
                if_exists,
            }
        }
    } else if cursor.keyword(b"INSTALL")? || cursor.keyword(b"UNINSTALL")? {
        // The first source word preserves which of the two commands matched.
        let install = super::super::first_keyword_in(sql, &[b"INSTALL"]);
        if !cursor.keyword(b"COMPONENT")? {
            return Ok(None);
        }
        let mut names = vec![cursor.account_text()?];
        while cursor.symbol(b',')? {
            names.push(cursor.account_text()?);
        }
        if install {
            Statement::InstallComponent { names }
        } else {
            Statement::UninstallComponent { names }
        }
    } else if cursor.keyword(b"RENAME")? {
        if !cursor.keyword(b"USER")? {
            return Ok(None);
        }
        let mut renames = Vec::new();
        loop {
            let from = cursor.account()?;
            cursor.expect_keyword(b"TO")?;
            renames.push((from, cursor.account()?));
            if !cursor.symbol(b',')? {
                break;
            }
        }
        Statement::RenameUser { renames }
    } else if cursor.keyword(b"SHOW")? {
        if cursor.keyword(b"GRANTS")? {
            if cursor.keyword(b"FOR")? {
                let account = current_or_account(&mut cursor)?;
                let using_roles = if cursor.keyword(b"USING")? {
                    cursor.accounts()?
                } else {
                    Vec::new()
                };
                Statement::ShowGrantsFor {
                    account,
                    using_roles,
                }
            } else {
                Statement::ShowGrants
            }
        } else if cursor.keyword(b"CREATE")? && cursor.keyword(b"USER")? {
            Statement::ShowCreateUser {
                account: current_or_account(&mut cursor)?,
            }
        } else {
            return Ok(None);
        }
    } else if cursor.keyword(b"SET")? {
        if !cursor.keyword(b"PASSWORD")? {
            return Ok(None);
        }
        let account = if cursor.keyword(b"FOR")? {
            Some(current_or_account(&mut cursor)?)
        } else {
            None
        };
        let random_password = cursor.keyword(b"TO")?;
        let password = if random_password {
            cursor.expect_keyword(b"RANDOM")?;
            String::new()
        } else {
            cursor.expect_symbol(b'=')?;
            if cursor
                .peek()?
                .is_some_and(|token| cursor.text(&token).eq_ignore_ascii_case("PASSWORD"))
            {
                return Err(cursor.expected("password literal; PASSWORD() is not supported"));
            }
            cursor.account_text()?
        };
        let mut replace = None;
        let mut retain_current = false;
        loop {
            if cursor.keyword(b"REPLACE")? {
                replace = Some(cursor.account_text()?);
            } else if cursor.keyword(b"RETAIN")? {
                cursor.expect_keyword(b"CURRENT")?;
                cursor.expect_keyword(b"PASSWORD")?;
                retain_current = true;
            } else {
                break;
            }
        }
        Statement::SetPassword {
            account,
            password,
            random_password,
            replace,
            retain_current,
        }
    } else {
        return Ok(None);
    };
    cursor.finish()?;
    Ok(Some(statement))
}
