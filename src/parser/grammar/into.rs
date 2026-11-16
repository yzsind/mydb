//! SELECT INTO user-variable targets, preserving token quotes and source spans.
use super::super::{
    current_parser_sql_mode_bits, parse_statement, ParseError, QueryExpr, SelectIntoFileKind,
    SelectIntoFileOptions, SelectIntoFilePosition, Span, Statement, MYSQL_VERSION_ID,
};
use super::cursor::Cursor;
use crate::sql_lex::SqlTokenKind;

pub(in crate::parser) fn parse_select_into_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let mut cursor = Cursor::new(sql);
    let mut depth = 0usize;
    let mut query_head = false;
    while let Some(token) = cursor.next()? {
        match token.kind {
            SqlTokenKind::Symbol(b'(') => depth += 1,
            SqlTokenKind::Symbol(b')') => depth = depth.saturating_sub(1),
            SqlTokenKind::Word if depth == 0 => {
                let word = cursor.text(&token);
                query_head |=
                    word.eq_ignore_ascii_case("SELECT") || word.eq_ignore_ascii_case("TABLE");
                if !word.eq_ignore_ascii_case("INTO") {
                    continue;
                }
                let next = cursor.peek()?;
                if next.as_ref().is_some_and(|next| {
                    next.kind == SqlTokenKind::Word
                        && ["OUTFILE", "DUMPFILE"]
                            .iter()
                            .any(|word| cursor.text(next).eq_ignore_ascii_case(word))
                }) {
                    let (inner, options) = strip_select_into_file(sql)?
                        .ok_or_else(|| cursor.expected("file export clause"))?;
                    let inner = QueryExpr::try_from(parse_statement(&inner)?)?;
                    return Ok(Some(Statement::SelectIntoFile { inner, options }));
                }
                if !query_head || !next.is_some_and(|next| next.kind == SqlTokenKind::Symbol(b'@'))
                {
                    continue;
                }
                let start = token.span.start;
                let mut targets = Vec::new();
                loop {
                    cursor.expect_symbol(b'@')?;
                    targets.push(variable_name(&mut cursor)?);
                    if !cursor.symbol(b',')? {
                        break;
                    }
                }
                let end = cursor.position();
                if let Some(tail) = cursor.peek()? {
                    let valid = tail.kind == SqlTokenKind::Symbol(b';')
                        || tail.kind == SqlTokenKind::Word
                            && [
                                "FROM",
                                "WHERE",
                                "GROUP",
                                "HAVING",
                                "WINDOW",
                                "ORDER",
                                "LIMIT",
                                "OFFSET",
                                "FOR",
                                "LOCK",
                                "UNION",
                                "INTERSECT",
                                "EXCEPT",
                            ]
                            .iter()
                            .any(|word| cursor.text(&tail).eq_ignore_ascii_case(word));
                    if !valid {
                        return Err(cursor.expected("query tail or end after INTO targets"));
                    }
                }
                let (inner, source_map) = super::super::source::edit_source(
                    sql,
                    &[super::super::source::TextEdit {
                        range: start..end,
                        replacement: " ",
                    }],
                )?;
                let inner = QueryExpr::try_from(
                    parse_statement(&inner).map_err(|error| source_map.error(sql, error))?,
                )?;
                return Ok(Some(Statement::SelectIntoVars { inner, targets }));
            }
            _ => {}
        }
    }
    Ok(None)
}

fn variable_name(cursor: &mut Cursor<'_>) -> Result<String, ParseError> {
    let token = cursor
        .peek()?
        .ok_or_else(|| cursor.expected("user variable name"))?;
    if matches!(
        token.kind,
        SqlTokenKind::String | SqlTokenKind::QuotedIdentifier
    ) {
        let name = cursor.quoted_text(&token)?;
        cursor.next()?;
        return Ok(name);
    }
    let mut name = String::new();
    let mut end = token.span.start;
    while let Some(part) = cursor.peek()? {
        if part.span.start != end
            || !matches!(
                part.kind,
                SqlTokenKind::Word | SqlTokenKind::Number | SqlTokenKind::Symbol(b'.' | b'$')
            )
        {
            break;
        }
        let raw = cursor.text(&part);
        if !raw
            .chars()
            .all(|ch| ch.is_alphanumeric() || matches!(ch, '_' | '.' | '$'))
        {
            break;
        }
        name.push_str(raw);
        end = part.span.end;
        cursor.next()?;
    }
    if name.is_empty() {
        return Err(ParseError::new(
            cursor.source(),
            Span {
                start: token.span.start,
                end: token.span.end,
            },
            "expected user variable name",
            vec!["user variable name".into()],
        ));
    }
    Ok(name)
}

/// Strip one top-level MySQL `INTO {OUTFILE|DUMPFILE}` clause and return the
/// ordinary query text plus its byte-format options. Token spans come from the
/// shared lexer, so quoted text, comments, executable comments and nested
/// subqueries cannot be mistaken for the export clause.
fn strip_select_into_file(
    sql: &str,
) -> Result<Option<(String, SelectIntoFileOptions)>, ParseError> {
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let mut tokens = Vec::new();
    while let Some(token) = cursor
        .next()
        .map_err(|err| crate::parser::entry::parse_lex_error(sql, err))?
    {
        tokens.push(token);
    }

    let mut depth = 0i32;
    let mut clause_index = None;
    for (index, token) in tokens.iter().enumerate() {
        match token.kind {
            crate::sql_lex::SqlTokenKind::Symbol(b'(') => depth += 1,
            crate::sql_lex::SqlTokenKind::Symbol(b')') => depth -= 1,
            crate::sql_lex::SqlTokenKind::Word if depth == 0 => {
                if sql[token.span.clone()].eq_ignore_ascii_case("INTO")
                    && tokens.get(index + 1).is_some_and(|next| {
                        next.kind == crate::sql_lex::SqlTokenKind::Word
                            && (sql[next.span.clone()].eq_ignore_ascii_case("OUTFILE")
                                || sql[next.span.clone()].eq_ignore_ascii_case("DUMPFILE"))
                    })
                {
                    if clause_index.replace(index).is_some() {
                        return Err(ParseError::new(
                            sql,
                            Span {
                                start: token.span.start,
                                end: token.span.end,
                            },
                            "multiple INTO OUTFILE/DUMPFILE clauses are not permitted",
                            Vec::new(),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    let Some(start_index) = clause_index else {
        return Ok(None);
    };

    let word_is = |index: usize, expected: &str| {
        tokens.get(index).is_some_and(|token| {
            token.kind == crate::sql_lex::SqlTokenKind::Word
                && sql[token.span.clone()].eq_ignore_ascii_case(expected)
        })
    };
    let syntax_error = |index: usize, message: &str| {
        let span = tokens
            .get(index)
            .map(|token| token.span.clone())
            .unwrap_or(sql.len()..sql.len());
        ParseError::new(
            sql,
            Span {
                start: span.start,
                end: span.end,
            },
            message,
            Vec::new(),
        )
    };
    let decode_string = |index: usize| -> Result<String, ParseError> {
        let token = tokens
            .get(index)
            .ok_or_else(|| syntax_error(index, "expected a string literal"))?;
        if token.kind != crate::sql_lex::SqlTokenKind::String {
            return Err(syntax_error(index, "expected a string literal"));
        }
        let mut literal_cursor =
            crate::sql_lex::SqlTokenCursor::new(&sql[token.span.clone()], mode, MYSQL_VERSION_ID);
        let literal = literal_cursor
            .next()
            .map_err(|err| crate::parser::entry::parse_lex_error(sql, err))?
            .ok_or_else(|| syntax_error(index, "expected string literal"))?;
        literal_cursor
            .decoded_quoted_text(&literal)
            .ok_or_else(|| syntax_error(index, "invalid string literal"))
    };
    let decode_name = |index: usize| -> Result<String, ParseError> {
        let token = tokens
            .get(index)
            .ok_or_else(|| syntax_error(index, "expected a character set name"))?;
        match token.kind {
            crate::sql_lex::SqlTokenKind::Word => Ok(sql[token.span.clone()].to_string()),
            crate::sql_lex::SqlTokenKind::QuotedIdentifier
            | crate::sql_lex::SqlTokenKind::String => {
                let mut name_cursor = crate::sql_lex::SqlTokenCursor::new(
                    &sql[token.span.clone()],
                    mode,
                    MYSQL_VERSION_ID,
                );
                let name_token = name_cursor
                    .next()
                    .map_err(|err| crate::parser::entry::parse_lex_error(sql, err))?
                    .expect("known quoted token");
                name_cursor
                    .decoded_quoted_text(&name_token)
                    .ok_or_else(|| syntax_error(index, "invalid character set name"))
            }
            _ => Err(syntax_error(index, "expected a character set name")),
        }
    };

    let kind = if word_is(start_index + 1, "OUTFILE") {
        SelectIntoFileKind::Outfile
    } else {
        SelectIntoFileKind::Dumpfile
    };
    let file_path = decode_string(start_index + 2)?;
    let mut index = start_index + 3;
    let mut charset = None;
    let mut field_terminated_by = "\t".to_string();
    let mut field_enclosed_by = None;
    let mut field_optionally_enclosed = false;
    let mut field_escaped_by = Some('\\');
    let mut line_terminated_by = "\n".to_string();
    let mut line_starting_by = String::new();

    if kind == SelectIntoFileKind::Outfile && word_is(index, "CHARACTER") {
        if !word_is(index + 1, "SET") {
            return Err(syntax_error(index + 1, "expected SET after CHARACTER"));
        }
        charset = Some(decode_name(index + 2)?);
        index += 3;
    }
    if kind == SelectIntoFileKind::Outfile
        && (word_is(index, "FIELDS") || word_is(index, "COLUMNS"))
    {
        index += 1;
        if word_is(index, "TERMINATED") {
            if !word_is(index + 1, "BY") {
                return Err(syntax_error(index + 1, "expected BY after TERMINATED"));
            }
            field_terminated_by = decode_string(index + 2)?;
            index += 3;
        }
        if word_is(index, "OPTIONALLY") {
            if !word_is(index + 1, "ENCLOSED") || !word_is(index + 2, "BY") {
                return Err(syntax_error(
                    index,
                    "expected OPTIONALLY ENCLOSED BY string",
                ));
            }
            let enclosure = decode_string(index + 3)?;
            if enclosure.chars().count() > 1 {
                return Err(syntax_error(
                    index + 3,
                    "ENCLOSED BY must be empty or a single character",
                ));
            }
            field_enclosed_by = enclosure.chars().next();
            field_optionally_enclosed = true;
            index += 4;
        } else if word_is(index, "ENCLOSED") {
            if !word_is(index + 1, "BY") {
                return Err(syntax_error(index + 1, "expected BY after ENCLOSED"));
            }
            let enclosure = decode_string(index + 2)?;
            if enclosure.chars().count() > 1 {
                return Err(syntax_error(
                    index + 2,
                    "ENCLOSED BY must be empty or a single character",
                ));
            }
            field_enclosed_by = enclosure.chars().next();
            index += 3;
        }
        if word_is(index, "ESCAPED") {
            if !word_is(index + 1, "BY") {
                return Err(syntax_error(index + 1, "expected BY after ESCAPED"));
            }
            let escape = decode_string(index + 2)?;
            if escape.chars().count() > 1 {
                return Err(syntax_error(
                    index + 2,
                    "ESCAPED BY must be empty or a single character",
                ));
            }
            field_escaped_by = escape.chars().next();
            index += 3;
        }
    }
    if kind == SelectIntoFileKind::Outfile && word_is(index, "LINES") {
        index += 1;
        if word_is(index, "STARTING") {
            if !word_is(index + 1, "BY") {
                return Err(syntax_error(index + 1, "expected BY after STARTING"));
            }
            line_starting_by = decode_string(index + 2)?;
            index += 3;
        }
        if word_is(index, "TERMINATED") {
            if !word_is(index + 1, "BY") {
                return Err(syntax_error(index + 1, "expected BY after TERMINATED"));
            }
            line_terminated_by = decode_string(index + 2)?;
            index += 3;
        }
    }

    let (position, clause_end) = if word_is(index, "FROM") {
        (
            SelectIntoFilePosition::AfterSelectList,
            tokens[index].span.start,
        )
    } else if word_is(index, "FOR") || word_is(index, "LOCK") {
        (
            SelectIntoFilePosition::BeforeLockingClause,
            tokens[index].span.start,
        )
    } else if tokens.get(index).is_none()
        || matches!(
            tokens[index].kind,
            crate::sql_lex::SqlTokenKind::Symbol(b';')
        )
    {
        (
            SelectIntoFilePosition::StatementEnd,
            tokens
                .get(index)
                .map_or(sql.len(), |token| token.span.start),
        )
    } else {
        let message = if kind == SelectIntoFileKind::Dumpfile {
            "DUMPFILE does not accept OUTFILE formatting options"
        } else {
            "invalid or out-of-order INTO OUTFILE option"
        };
        return Err(syntax_error(index, message));
    };

    let clause_start = tokens[start_index].span.start;
    let mut inner = String::with_capacity(sql.len() - (clause_end - clause_start) + 1);
    inner.push_str(&sql[..clause_start]);
    inner.push(' ');
    inner.push_str(&sql[clause_end..]);
    Ok(Some((
        inner.trim().to_string(),
        SelectIntoFileOptions {
            kind,
            file_path,
            charset,
            field_terminated_by,
            field_enclosed_by,
            field_optionally_enclosed,
            field_escaped_by,
            line_terminated_by,
            line_starting_by,
            position,
        },
    )))
}
