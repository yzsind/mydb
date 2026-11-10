//! Token-based grammar for SHOW forms that need stricter MySQL boundaries.
use super::super::{ParseError, ShowEngineKind, Statement};
use super::cursor::Cursor;
use crate::sql_lex::SqlTokenKind;

pub(in crate::parser) fn parse_show_engine(sql: &str) -> Result<Option<Statement>, ParseError> {
    let mut c = Cursor::new(sql);
    if !c.keyword(b"SHOW")? || !c.keyword(b"ENGINE")? {
        return Ok(None);
    }
    let token = c.next()?.ok_or_else(|| c.expected("engine name"))?;
    let engine = match token.kind {
        SqlTokenKind::Word => c.text(&token).to_owned(),
        SqlTokenKind::QuotedIdentifier | SqlTokenKind::String => c.quoted_text(&token)?,
        _ => return Err(c.expected("engine name")),
    };
    let kind = if c.keyword(b"STATUS")? {
        ShowEngineKind::Status
    } else if c.keyword(b"MUTEX")? {
        ShowEngineKind::Mutex
    } else {
        // Preserve the existing unsupported-SHOW diagnostic for unknown forms.
        return Ok(None);
    };
    c.finish()?;
    if engine.eq_ignore_ascii_case("mydb") && kind == ShowEngineKind::Status {
        Ok(Some(Statement::ShowEngineMydbStatus))
    } else {
        Ok(Some(Statement::ShowEngine { engine, kind }))
    }
}

fn profile_number(c: &mut Cursor<'_>, max: u64) -> Result<(), ParseError> {
    let token = c.next()?.ok_or_else(|| c.expected("unsigned integer"))?;
    let text = c.text(&token);
    if token.kind != SqlTokenKind::Number
        || !text.bytes().all(|byte| byte.is_ascii_digit())
        || text.parse::<u64>().map_or(true, |value| value > max)
    {
        return Err(c.expected("unsigned integer"));
    }
    Ok(())
}

fn profile_type(c: &mut Cursor<'_>) -> Result<(), ParseError> {
    if c.keyword(b"BLOCK")? {
        c.expect_keyword(b"IO")
    } else if c.keyword(b"CONTEXT")? {
        c.expect_keyword(b"SWITCHES")
    } else if c.keyword(b"PAGE")? {
        c.expect_keyword(b"FAULTS")
    } else if c.keyword(b"CPU")?
        || c.keyword(b"MEMORY")?
        || c.keyword(b"IPC")?
        || c.keyword(b"SWAPS")?
        || c.keyword(b"SOURCE")?
        || c.keyword(b"ALL")?
    {
        Ok(())
    } else {
        Err(c.expected("SHOW PROFILE type"))
    }
}

/// Shared by parsing, execution and prepared-result metadata classification.
pub(crate) fn validate_show_profile(sql: &str) -> Result<(), ParseError> {
    let mut c = Cursor::new(sql);
    c.expect_keyword(b"SHOW")?;
    c.expect_keyword(b"PROFILE")?;
    if c.peek()?.is_some_and(|token| {
        token.kind == SqlTokenKind::Word
            && !["FOR", "LIMIT"]
                .iter()
                .any(|word| c.text(&token).eq_ignore_ascii_case(word))
    }) {
        profile_type(&mut c)?;
        while c.symbol(b',')? {
            profile_type(&mut c)?;
        }
    }
    if c.keyword(b"FOR")? {
        c.expect_keyword(b"QUERY")?;
        profile_number(&mut c, i32::MAX as u64)?;
    }
    if c.keyword(b"LIMIT")? {
        profile_number(&mut c, u64::MAX)?;
        if c.symbol(b',')? || c.keyword(b"OFFSET")? {
            profile_number(&mut c, u64::MAX)?;
        }
    }
    c.finish()
}

pub(in crate::parser) fn parse_show_profile(sql: &str) -> Result<Option<Statement>, ParseError> {
    let mut c = Cursor::new(sql);
    if !c.keyword(b"SHOW")? {
        return Ok(None);
    }
    if !c.keyword(b"PROFILE")? {
        if c.peek()?.is_some_and(|token| {
            token.kind == SqlTokenKind::Word
                && c.text(&token)
                    .get(.."PROFILE".len())
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case("PROFILE"))
                && !c.text(&token).eq_ignore_ascii_case("PROFILES")
        }) {
            return Err(c.expected("PROFILE or PROFILES"));
        }
        return Ok(None);
    }
    validate_show_profile(sql)?;
    // The admin classifier expects word boundaries even when comments separate them.
    let mode =
        crate::sql_lex::SqlLexMode::from_parser_bits(super::super::current_parser_sql_mode_bits());
    let cleaned = crate::sql_lex::mask_comments(sql, mode, super::super::MYSQL_VERSION_ID, false)
        .map_err(|err| super::super::parse_lex_error(sql, err))?;
    Ok(Some(Statement::BinlogAdmin {
        sql: crate::sql_lex::normalize_admin_sql(&cleaned),
    }))
}
