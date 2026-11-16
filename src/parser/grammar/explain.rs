//! EXPLAIN compatibility syntax, sharing the parser's quote-aware token cursor.
use std::borrow::Cow;

use super::super::{
    current_parser_sql_mode_bits, parse_lex_error, parse_statement,
    try_parse_plain_query_or_delete, ExplainFormat, ParseError, SourceMap, Statement, TextEdit,
    MYSQL_VERSION_ID,
};
use super::cursor::Cursor;
use crate::sql_lex::{segments, SqlLexMode, SqlSegmentKind, SqlTokenKind};

/// Locate the body once for both header comment masking and TABLE lowering.
fn normalize(sql: &str, lower_table: bool) -> Result<(Cow<'_, str>, SourceMap), ParseError> {
    let mut cursor = Cursor::new(sql);
    let explain = cursor.next().ok().flatten().is_some_and(|token| {
        token.kind == SqlTokenKind::Word
            && ["EXPLAIN", "DESC", "DESCRIBE"]
                .iter()
                .any(|word| cursor.text(&token).eq_ignore_ascii_case(word))
    });
    if !explain {
        return Ok((Cow::Borrowed(sql), SourceMap::slice(sql, 0..sql.len())));
    }
    let body = loop {
        let Some(token) = cursor.peek()? else {
            break None;
        };
        let modifier = token.kind == SqlTokenKind::Symbol(b'=')
            || (token.kind == SqlTokenKind::Word
                && [
                    "ANALYZE",
                    "EXTENDED",
                    "PARTITIONS",
                    "VERBOSE",
                    "FORMAT",
                    "TRADITIONAL",
                    "JSON",
                    "TREE",
                ]
                .iter()
                .any(|word| cursor.text(&token).eq_ignore_ascii_case(word)));
        if !modifier {
            break Some(token);
        }
        cursor.next()?;
    };
    let header_end = body.as_ref().map_or(sql.len(), |token| token.span.start);
    let table = body.filter(|token| {
        lower_table
            && token.kind == SqlTokenKind::Word
            && cursor.text(token).eq_ignore_ascii_case("TABLE")
    });
    let copy_source = || {
        let mut bytes = Vec::with_capacity(sql.len() + if table.is_some() { 8 } else { 0 });
        bytes.extend_from_slice(sql.as_bytes());
        bytes
    };
    let mut output = None;
    let header = &sql[..header_end];
    // The cursor has validated the header. Without a comment introducer,
    // there is no masking work for a second lexical pass.
    if memchr::memchr3(b'/', b'-', b'#', header.as_bytes()).is_some() {
        for segment in segments(
            header,
            SqlLexMode::from_parser_bits(current_parser_sql_mode_bits()),
            MYSQL_VERSION_ID,
        ) {
            let segment = segment.map_err(|err| parse_lex_error(sql, err))?;
            if matches!(
                segment.kind,
                SqlSegmentKind::LineComment
                    | SqlSegmentKind::BlockComment
                    | SqlSegmentKind::OptimizerHint
            ) {
                let bytes = output.get_or_insert_with(&copy_source);
                for byte in &mut bytes[segment.range] {
                    if !matches!(*byte, b'\r' | b'\n') {
                        *byte = b' ';
                    }
                }
            }
        }
    }
    if let Some(token) = &table {
        output
            .get_or_insert_with(copy_source)
            .splice(token.span.clone(), b"SELECT * FROM".iter().copied());
    }
    let edit = table.map(|token| TextEdit {
        range: token.span,
        replacement: "SELECT * FROM",
    });
    let map = SourceMap::for_edits(sql, edit.as_slice())?;
    let output = match output {
        Some(bytes) => Cow::Owned(String::from_utf8(bytes).expect("SQL masking preserves UTF-8")),
        None => Cow::Borrowed(sql),
    };
    Ok((output, map))
}

pub(in crate::parser) fn normalize_explain(
    sql: &str,
) -> Result<(Cow<'_, str>, SourceMap), ParseError> {
    normalize(sql, true)
}

/// Session cache normalization must leave the explained body untouched.
/// Invalid input is left for the parser to diagnose.
pub(crate) fn strip_comments_in_explain_header(sql: &str) -> Cow<'_, str> {
    normalize(sql, false)
        .map(|(sql, _)| sql)
        .unwrap_or(Cow::Borrowed(sql))
}

/// sqlparser lacks FOR CONNECTION and the TRADITIONAL runtime metrics format.
/// Native TREE/JSON statements stay on its regular grammar path.
pub(in crate::parser) fn parse_explain_extension(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    parse_extension(sql, true)
}

/// The fixed connection-id form never parses an expression or target query.
pub(in crate::parser) fn parse_explain_connection(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    parse_extension(sql, false)
}

fn parse_extension(sql: &str, allow_target: bool) -> Result<Option<Statement>, ParseError> {
    let mut cursor = Cursor::new(sql);
    // The same cursor both gates the grammar and consumes its head token.
    // Invalid heads remain for the canonical parser to diagnose.
    if !cursor.keyword(b"EXPLAIN").unwrap_or(false) {
        return Ok(None);
    }
    let analyze = cursor.keyword(b"ANALYZE")?;
    let mut format_has_equals = true;
    let format = if cursor.keyword(b"FORMAT")? {
        format_has_equals = cursor.symbol(b'=')?;
        Some(if cursor.keyword(b"TRADITIONAL")? {
            ExplainFormat::Traditional
        } else if cursor.keyword(b"JSON")? {
            ExplainFormat::Json
        } else if cursor.keyword(b"TREE")? {
            ExplainFormat::Tree
        } else {
            return Err(cursor.expected("TRADITIONAL, JSON or TREE"));
        })
    } else {
        None
    };
    if !analyze && cursor.keyword(b"FOR")? {
        cursor.expect_keyword(b"CONNECTION")?;
        let token = cursor
            .peek()?
            .ok_or_else(|| cursor.expected("connection id"))?;
        let connection_id = if token.kind == SqlTokenKind::Number {
            cursor.text(&token).parse::<u32>().ok()
        } else {
            None
        }
        .ok_or_else(|| cursor.expected("connection id in 0..=4294967295"))?;
        cursor.next()?;
        cursor.finish()?;
        return Ok(Some(Statement::ExplainForConnection {
            connection_id,
            format,
        }));
    }
    if !allow_target {
        return Ok(None);
    }
    let token = cursor
        .peek()?
        .ok_or_else(|| cursor.expected("EXPLAIN target statement"))?;
    let start = token.span.start;
    // A simple SELECT has no compatibility metadata outside its own AST.
    // Reuse that proved subset instead of running every DDL/DML rewrite on
    // the EXPLAIN wrapper. Other targets retain the canonical outer grammar.
    if (format_has_equals || matches!(format, Some(ExplainFormat::Traditional)))
        && token.kind == SqlTokenKind::Word
        && cursor.text(&token).eq_ignore_ascii_case("SELECT")
    {
        if let Ok(Some(target @ Statement::Select(_))) =
            try_parse_plain_query_or_delete(&sql[start..])
        {
            return Ok(Some(Statement::Explain {
                target: Box::new(target),
                format,
                analyze,
            }));
        }
    }
    if !matches!(format, Some(ExplainFormat::Traditional)) {
        return Ok(None);
    }
    let target = parse_statement(&sql[start..])
        .map_err(|err| SourceMap::slice(sql, start..sql.len()).error(sql, err))?;
    Ok(Some(Statement::Explain {
        target: Box::new(target),
        format,
        analyze,
    }))
}
