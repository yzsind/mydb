use super::super::{ParseError, SetDefaultRole, SetRole, Statement};
use super::cursor::Cursor;

pub(in crate::parser) fn parse_role_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let mut cursor = Cursor::new(sql);
    if !cursor.keyword(b"SET")? {
        return Ok(None);
    }
    if cursor.keyword(b"DEFAULT")? {
        if !cursor.keyword(b"ROLE")? {
            return Ok(None);
        }
        let kind = if cursor.keyword(b"ALL")? {
            0
        } else if cursor.keyword(b"NONE")? {
            1
        } else {
            2
        };
        let roles = if kind == 2 {
            cursor.accounts()?
        } else {
            Vec::new()
        };
        cursor.expect_keyword(b"TO")?;
        let to = cursor.accounts()?;
        cursor.finish()?;
        return Ok(Some(Statement::SetDefaultRole(match kind {
            0 => SetDefaultRole::All { to },
            1 => SetDefaultRole::None { to },
            _ => SetDefaultRole::Roles { roles, to },
        })));
    }
    if !cursor.keyword(b"ROLE")? {
        return Ok(None);
    }
    let role = role_choice(&mut cursor)?;
    cursor.finish()?;
    Ok(Some(Statement::SetRole(role)))
}

pub(super) fn role_choice(cursor: &mut Cursor<'_>) -> Result<SetRole, ParseError> {
    Ok(if cursor.keyword(b"ALL")? {
        if cursor.keyword(b"EXCEPT")? {
            SetRole::AllExcept(cursor.accounts()?)
        } else {
            SetRole::All
        }
    } else if cursor.keyword(b"NONE")? {
        SetRole::None
    } else if cursor.keyword(b"DEFAULT")? {
        SetRole::Default
    } else {
        SetRole::Roles(cursor.accounts()?)
    })
}
