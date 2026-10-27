use super::super::{
    ParseError, SetScope, Statement, TransactionCharacteristic, TransactionIsolation,
};
use super::cursor::Cursor;
use crate::sql_lex::SqlTokenKind;

/// Transaction control uses one token grammar, including quoted savepoint names.
pub(in crate::parser) fn parse_transaction_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let mut cursor = Cursor::new(sql);
    let commit = cursor.keyword(b"COMMIT")?;
    if !commit && !cursor.keyword(b"ROLLBACK")? {
        if cursor.keyword(b"BEGIN")? {
            cursor.keyword(b"WORK")?;
            return parse_begin(&mut cursor).map(Some);
        }
        if cursor.keyword(b"START")? {
            if !cursor.keyword(b"TRANSACTION")? {
                return Ok(None);
            }
            return parse_begin(&mut cursor).map(Some);
        }
        let release = cursor.keyword(b"RELEASE")?;
        if !cursor.keyword(b"SAVEPOINT")? {
            return Ok(None);
        }
        let name = cursor.identifier()?;
        cursor.finish()?;
        return Ok(Some(if release {
            Statement::ReleaseSavepoint(name)
        } else {
            Statement::Savepoint(name)
        }));
    }
    let _ = cursor.keyword(b"WORK")?;
    if !commit && cursor.keyword(b"TO")? {
        let _ = cursor.keyword(b"SAVEPOINT")?;
        let name = cursor.identifier()?;
        cursor.finish()?;
        return Ok(Some(Statement::RollbackToSavepoint(name)));
    }
    let chain = if cursor.keyword(b"AND")? {
        let no = cursor.keyword(b"NO")?;
        cursor.expect_keyword(b"CHAIN")?;
        Some(!no)
    } else {
        None
    };
    let release = if cursor.keyword(b"NO")? {
        cursor.expect_keyword(b"RELEASE")?;
        Some(false)
    } else if cursor.keyword(b"RELEASE")? {
        Some(true)
    } else {
        None
    };
    cursor.finish()?;
    Ok(Some(if commit {
        Statement::Commit { chain, release }
    } else {
        Statement::Rollback { chain, release }
    }))
}

fn read_access(cursor: &mut Cursor<'_>) -> Result<bool, ParseError> {
    if cursor.keyword(b"ONLY")? {
        Ok(true)
    } else {
        cursor.expect_keyword(b"WRITE")?;
        Ok(false)
    }
}

fn parse_begin(cursor: &mut Cursor<'_>) -> Result<Statement, ParseError> {
    let mut read_only = None;
    let mut consistent_snapshot = false;
    if cursor
        .peek()?
        .is_some_and(|token| token.kind != SqlTokenKind::Symbol(b';'))
    {
        loop {
            if cursor.keyword(b"WITH")? {
                cursor.expect_keyword(b"CONSISTENT")?;
                cursor.expect_keyword(b"SNAPSHOT")?;
                consistent_snapshot = true;
            } else {
                cursor.expect_keyword(b"READ")?;
                let value = read_access(cursor)?;
                if read_only.is_some_and(|previous| previous != value) {
                    return Err(cursor.expected("one transaction access mode"));
                }
                read_only = Some(value);
            }
            if !cursor.symbol(b',')? {
                break;
            }
        }
    }
    cursor.finish()?;
    Ok(Statement::Begin {
        read_only,
        consistent_snapshot,
    })
}

fn characteristic(cursor: &mut Cursor<'_>) -> Result<TransactionCharacteristic, ParseError> {
    if cursor.keyword(b"READ")? {
        return read_access(cursor).map(TransactionCharacteristic::ReadOnly);
    }
    cursor.expect_keyword(b"ISOLATION")?;
    cursor.expect_keyword(b"LEVEL")?;
    let isolation = if cursor.keyword(b"READ")? {
        if cursor.keyword(b"UNCOMMITTED")? {
            TransactionIsolation::ReadUncommitted
        } else {
            cursor.expect_keyword(b"COMMITTED")?;
            TransactionIsolation::ReadCommitted
        }
    } else if cursor.keyword(b"REPEATABLE")? {
        cursor.expect_keyword(b"READ")?;
        TransactionIsolation::RepeatableRead
    } else {
        cursor.expect_keyword(b"SERIALIZABLE")?;
        TransactionIsolation::Serializable
    };
    Ok(TransactionCharacteristic::Isolation(isolation))
}

/// SET's scoped session forms share their prefix; generic assignments fall
/// through intact so expression values and multi-assignment semantics stay native.
pub(in crate::parser) fn parse_set_session_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let mut cursor = Cursor::new(sql);
    if !cursor.keyword(b"SET")? {
        return Ok(None);
    }
    let scope = if cursor.keyword(b"SESSION")? || cursor.keyword(b"LOCAL")? {
        Some(SetScope::Session)
    } else if cursor.keyword(b"GLOBAL")? {
        Some(SetScope::Global)
    } else {
        None
    };
    if cursor.keyword(b"TRANSACTION")? {
        let first = characteristic(&mut cursor)?;
        if !cursor.symbol(b',')? {
            cursor.finish()?;
            return Ok(Some(match first {
                TransactionCharacteristic::Isolation(isolation) => {
                    Statement::SetTransactionIsolation { scope, isolation }
                }
                _ => Statement::SetTransactionCharacteristics {
                    scope,
                    characteristics: vec![first],
                },
            }));
        }
        let mut characteristics = vec![first];
        loop {
            characteristics.push(characteristic(&mut cursor)?);
            if !cursor.symbol(b',')? {
                break;
            }
        }
        cursor.finish()?;
        let isolation_count = characteristics
            .iter()
            .filter(|value| matches!(value, TransactionCharacteristic::Isolation(_)))
            .count();
        let access_count = characteristics.len() - isolation_count;
        if isolation_count > 1 || access_count > 1 {
            return Err(cursor.expected("at most one isolation level and one access mode"));
        }
        return Ok(Some(Statement::SetTransactionCharacteristics {
            scope,
            characteristics,
        }));
    }
    let charset = if cursor.keyword(b"CHARACTER")? {
        cursor.expect_keyword(b"SET")?;
        true
    } else {
        cursor.keyword(b"CHARSET")?
    };
    if charset {
        let token = cursor
            .peek()?
            .ok_or_else(|| cursor.expected("charset name or DEFAULT"))?;
        let name = match token.kind {
            SqlTokenKind::Word => cursor.text(&token).to_owned(),
            SqlTokenKind::String | SqlTokenKind::QuotedIdentifier => cursor.quoted_text(&token)?,
            _ => return Err(cursor.expected("charset name or DEFAULT")),
        };
        cursor.next()?;
        cursor.finish()?;
        return Ok(Some(Statement::SetCharacterSet {
            charset: if name.eq_ignore_ascii_case("DEFAULT") {
                None
            } else {
                Some(name)
            },
        }));
    }
    if scope == Some(SetScope::Global) {
        return Ok(None);
    }
    if cursor.symbol(b'@')? && !cursor.symbol(b'@')? {
        return Ok(None);
    }
    if cursor.keyword(b"SESSION")? || cursor.keyword(b"LOCAL")? {
        if !cursor.symbol(b'.')? {
            return Ok(None);
        }
    }
    if !cursor.keyword(b"AUTOCOMMIT")? || !cursor.symbol(b'=')? {
        return Ok(None);
    }
    let Some(token) = cursor.next()? else {
        return Ok(None);
    };
    let value = match token.kind {
        SqlTokenKind::Word | SqlTokenKind::Number => cursor.text(&token),
        _ => return Ok(None),
    };
    let enabled =
        if value == "1" || value.eq_ignore_ascii_case("ON") || value.eq_ignore_ascii_case("TRUE") {
            true
        } else if value == "0"
            || value.eq_ignore_ascii_case("OFF")
            || value.eq_ignore_ascii_case("FALSE")
        {
            false
        } else {
            return Ok(None);
        };
    // Expressions or other assignments belong to the normal SET converter.
    cursor.symbol(b';')?;
    if cursor.peek()?.is_some() {
        return Ok(None);
    }
    Ok(Some(Statement::SetAutocommit(enabled)))
}
