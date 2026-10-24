//! View headers and query bodies are parsed once, with typed lexical options.
use super::super::{
    current_parser_sql_mode_bits, edit_source, parse_lex_error, parse_statement, ParseError,
    QueryExpr, RoutineSecurity, SourceMap, Statement, TextEdit, MYSQL_VERSION_ID,
};
use super::cursor::Cursor;
use crate::sql_lex::{SqlLexMode, SqlToken, SqlTokenCursor, SqlTokenKind};
use std::collections::VecDeque;

pub(in crate::parser) fn parse_view_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let mut cursor = Cursor::new(sql);
    let create = cursor.keyword(b"CREATE")?;
    if !create && !cursor.keyword(b"ALTER")? {
        return Ok(None);
    }
    let or_replace = if create && cursor.keyword(b"OR")? {
        cursor.expect_keyword(b"REPLACE")?;
        true
    } else {
        false
    };
    // Other CREATE/ALTER families belong to their own grammar modules.
    let Some(token) = cursor.peek()? else {
        return Ok(None);
    };
    if token.kind != SqlTokenKind::Word
        || !["VIEW", "ALGORITHM", "DEFINER", "SQL"]
            .iter()
            .any(|word| cursor.text(&token).eq_ignore_ascii_case(word))
    {
        return Ok(None);
    }
    let mut algorithm = None;
    let mut definer = None;
    let mut security_type = None;
    let mut options_seen = 0u8;
    loop {
        let flag = if cursor.keyword(b"ALGORITHM")? {
            cursor.expect_symbol(b'=')?;
            let value = cursor.identifier()?;
            if !["UNDEFINED", "MERGE", "TEMPTABLE"]
                .iter()
                .any(|candidate| value.eq_ignore_ascii_case(candidate))
            {
                return Err(cursor.expected("UNDEFINED, MERGE or TEMPTABLE"));
            }
            algorithm = Some(value.to_ascii_uppercase());
            1
        } else if cursor.keyword(b"DEFINER")? {
            cursor.expect_symbol(b'=')?;
            if cursor.keyword(b"CURRENT_USER")? {
                if cursor.symbol(b'(')? {
                    cursor.expect_symbol(b')')?;
                }
            } else {
                definer = Some(cursor.account()?);
            }
            2
        } else if cursor.keyword(b"SQL")? {
            cursor.expect_keyword(b"SECURITY")?;
            security_type = Some(if cursor.keyword(b"DEFINER")? {
                RoutineSecurity::Definer
            } else if cursor.keyword(b"INVOKER")? {
                RoutineSecurity::Invoker
            } else {
                return Err(cursor.expected("DEFINER or INVOKER"));
            });
            4
        } else {
            break;
        };
        if options_seen & flag != 0 {
            return Err(cursor.expected("a view option specified once"));
        }
        options_seen |= flag;
    }
    if options_seen == 2
        && cursor.peek()?.is_some_and(|token| {
            token.kind == SqlTokenKind::Word
                && ["PROCEDURE", "FUNCTION", "TRIGGER", "EVENT"]
                    .iter()
                    .any(|word| cursor.text(&token).eq_ignore_ascii_case(word))
        })
    {
        return Ok(None);
    }
    cursor.expect_keyword(b"VIEW")?;
    let if_not_exists = if create && cursor.keyword(b"IF")? {
        cursor.expect_keyword(b"NOT")?;
        cursor.expect_keyword(b"EXISTS")?;
        true
    } else {
        false
    };
    let name = cursor.qualified_name()?;
    let mut columns = Vec::new();
    if cursor.symbol(b'(')? {
        columns.push(cursor.identifier()?);
        while cursor.symbol(b',')? {
            columns.push(cursor.identifier()?);
        }
        cursor.expect_symbol(b')')?;
    }
    cursor.expect_keyword(b"AS")?;
    let after_as = cursor.position();
    let raw_body = &sql[after_as..];
    let body_offset = after_as + raw_body.len() - raw_body.trim_start().len();
    let body_source = &sql[body_offset..];
    let (body, check_option, body_map) = query_body(body_source)
        .map_err(|error| SourceMap::slice(sql, body_offset..sql.len()).error(sql, error))?;
    let query = parse_statement(&body).map_err(|error| {
        SourceMap::slice(sql, body_offset..sql.len()).error(sql, body_map.error(body_source, error))
    })?;
    let query = QueryExpr::try_from(query)
        .map_err(|error| SourceMap::slice(sql, body_offset..sql.len()).error(sql, error))?;
    Ok(Some(if create {
        Statement::CreateView {
            name,
            sql: body,
            query,
            or_replace,
            if_not_exists,
            algorithm,
            definer,
            security_type,
            columns,
            check_option,
        }
    } else {
        Statement::AlterView {
            name,
            sql: body,
            query,
            algorithm,
            definer,
            security_type,
            columns,
            check_option,
        }
    }))
}

/// Only the final unquoted token sequence may be WITH [LOCAL|CASCADED]
/// CHECK OPTION. The original body remains intact when no suffix matches.
fn query_body(source: &str) -> Result<(String, Option<String>, SourceMap), ParseError> {
    let mut cursor = SqlTokenCursor::new(
        source,
        SqlLexMode::from_parser_bits(current_parser_sql_mode_bits()),
        MYSQL_VERSION_ID,
    );
    let mut tail: VecDeque<SqlToken> = VecDeque::with_capacity(5);
    while let Some(token) = cursor
        .next()
        .map_err(|error| parse_lex_error(source, error))?
    {
        if tail.len() == 5 {
            tail.pop_front();
        }
        tail.push_back(token);
    }
    if tail
        .back()
        .is_some_and(|token| token.kind == SqlTokenKind::Symbol(b';'))
    {
        tail.pop_back();
    }
    let tail: Vec<_> = tail.into_iter().collect();
    let word = |index: usize, expected: &str| {
        tail.get(index).is_some_and(|token| {
            token.kind == SqlTokenKind::Word
                && cursor.token_text(token).eq_ignore_ascii_case(expected)
        })
    };
    let count = tail.len();
    if count < 3 || !word(count - 1, "OPTION") || !word(count - 2, "CHECK") {
        return Ok((
            source.to_owned(),
            None,
            SourceMap::slice(source, 0..source.len()),
        ));
    }
    let (start, option) = if word(count - 3, "WITH") {
        (count - 3, "CASCADED")
    } else if count >= 4 && word(count - 4, "WITH") {
        if word(count - 3, "LOCAL") {
            (count - 4, "LOCAL")
        } else if word(count - 3, "CASCADED") {
            (count - 4, "CASCADED")
        } else {
            return Ok((
                source.to_owned(),
                None,
                SourceMap::slice(source, 0..source.len()),
            ));
        }
    } else {
        return Ok((
            source.to_owned(),
            None,
            SourceMap::slice(source, 0..source.len()),
        ));
    };
    let (body, map) = edit_source(
        source,
        &[TextEdit {
            range: tail[start].span.start..tail[count - 1].span.end,
            replacement: "",
        }],
    )?;
    Ok((body.into_owned(), Some(option.to_owned()), map))
}
