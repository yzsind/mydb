//! sqlparser carries parameter identity in Placeholder(String). Assign a
//! lexical index before parsing, so even AST variants that discard value
//! spans retain the identity. Only the adapter sees indexed `?N` spellings.
use sqlparser::tokenizer::{Token, TokenWithSpan};
use std::fmt::Write;

pub(super) fn assign(tokens: &mut [TokenWithSpan]) -> Result<(), sqlparser::parser::ParserError> {
    let mut index = 0;
    for token in tokens {
        if let Token::Placeholder(name) = &mut token.token {
            if name != "?" {
                return Err(sqlparser::parser::ParserError::ParserError(format!(
                    "only anonymous ? parameters are supported at Line: {}, Column: {}",
                    token.span.start.line, token.span.start.column,
                )));
            }
            write!(name, "{index}").expect("writing into a String");
            index += 1;
        }
    }
    Ok(())
}

pub(in crate::parser) fn index(name: &str) -> Result<usize, crate::parser::ParseError> {
    name.strip_prefix('?')
        .and_then(|index| index.parse().ok())
        .ok_or_else(|| crate::parser::ParseError::at_zero("parameter has no lexical identity"))
}

/// sqlparser display is still used at a few legacy presentation/persistence
/// boundaries. Anonymous parameters must remain `?`, including inside TRIM;
/// quoted strings and identifiers containing question marks remain intact.
pub(in crate::parser) fn display(value: &impl std::fmt::Display) -> String {
    let rendered = value.to_string();
    if !rendered.contains('?') {
        return rendered;
    }
    let mode =
        crate::sql_lex::SqlLexMode::from_parser_bits(crate::parser::current_parser_sql_mode_bits());
    let mut tokens =
        crate::sql_lex::SqlTokenCursor::new(&rendered, mode, crate::parser::MYSQL_VERSION_ID);
    let mut ranges = Vec::new();
    while let Ok(Some(token)) = tokens.next() {
        if token.kind == crate::sql_lex::SqlTokenKind::Placeholder {
            let start = token.span.end;
            let mut end = start;
            while rendered.as_bytes().get(end).is_some_and(u8::is_ascii_digit) {
                end += 1;
            }
            if end > start {
                ranges.push(start..end);
            }
        }
    }
    if ranges.is_empty() {
        return rendered;
    }
    let mut output = String::with_capacity(rendered.len());
    let mut copied = 0;
    for range in ranges {
        output.push_str(&rendered[copied..range.start]);
        copied = range.end;
    }
    output.push_str(&rendered[copied..]);
    output
}
