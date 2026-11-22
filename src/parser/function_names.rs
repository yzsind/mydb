//! MySQL distinguishes keyword functions, lexer-sensitive functions, and
//! ordinary function names before it binds identifiers or stored routines.
use sqlparser::{
    ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArgOperator, FunctionArguments, Ident},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Token, TokenWithSpan},
};

use super::entry::current_parser_sql_mode_bits;
use super::{parse_error_from_sql_span, raw_source_byte_span, with_parser_source, ParseError};
use crate::engine::api::SQL_MODE_IGNORE_SPACE;

#[derive(Clone, Copy)]
enum FunctionToken {
    Keyword {
        precision: bool,
        optional_parentheses: bool,
    },
    Adjacent {
        precision: Option<bool>,
        reserved: bool,
    },
}

impl FunctionToken {
    fn has_precision(self) -> bool {
        matches!(
            self,
            Self::Keyword {
                precision: true,
                ..
            } | Self::Adjacent {
                precision: Some(true),
                ..
            }
        )
    }

    fn reserved_as_identifier(self, call_follows: bool) -> bool {
        matches!(self, Self::Keyword { .. })
            || (call_follows && matches!(self, Self::Adjacent { reserved: true, .. }))
    }
}

/// The keyword aliases and SYM_FN entries in MySQL 8.0's sql/lex.h.
/// A few SYM_FN tokens also occur in the grammar's identifier productions.
fn classify(name: &str) -> Option<FunctionToken> {
    use FunctionToken::{Adjacent, Keyword};
    let mut upper = [0; b"CURRENT_TIMESTAMP".len()];
    let word = upper.get_mut(..name.len())?;
    word.copy_from_slice(name.as_bytes());
    word.make_ascii_uppercase();
    let (precision, reserved) = match &*word {
        b"CURRENT_DATE" | b"CURRENT_USER" | b"UTC_DATE" => {
            return Some(Keyword {
                precision: false,
                optional_parentheses: true,
            })
        }
        b"CURRENT_TIME" | b"CURRENT_TIMESTAMP" | b"LOCALTIME" | b"LOCALTIMESTAMP" | b"UTC_TIME"
        | b"UTC_TIMESTAMP" => {
            return Some(Keyword {
                precision: true,
                optional_parentheses: true,
            })
        }
        b"DATABASE" | b"SCHEMA" => {
            return Some(Keyword {
                precision: false,
                optional_parentheses: false,
            })
        }
        b"CURDATE" => (Some(false), true),
        b"CURTIME" | b"NOW" | b"SYSDATE" => (Some(true), true),
        b"ADDDATE" | b"SUBDATE" | b"SESSION_USER" | b"SYSTEM_USER" | b"ST_COLLECT" => (None, false),
        b"BIT_AND" | b"BIT_OR" | b"BIT_XOR" | b"CAST" | b"COUNT" | b"DATE_ADD" | b"DATE_SUB"
        | b"EXTRACT" | b"GROUP_CONCAT" | b"JSON_ARRAYAGG" | b"JSON_OBJECTAGG" | b"MAX" | b"MID"
        | b"MIN" | b"POSITION" | b"STD" | b"STDDEV" | b"STDDEV_POP" | b"STDDEV_SAMP"
        | b"SUBSTR" | b"SUBSTRING" | b"SUM" | b"TRIM" | b"VARIANCE" | b"VAR_POP" | b"VAR_SAMP" => {
            (None, true)
        }
        _ => return None,
    };
    Some(Adjacent {
        precision,
        reserved,
    })
}

#[inline]
fn can_be_keyword_function(name: &str) -> bool {
    name.len() >= 6
        && matches!(
            name.as_bytes()[0].to_ascii_uppercase(),
            b'C' | b'D' | b'L' | b'S' | b'U'
        )
}

fn ignore_space() -> bool {
    current_parser_sql_mode_bits() & SQL_MODE_IGNORE_SPACE != 0
}

pub(super) fn reserved_identifier(name: &str, source: &str, start: usize, end: usize) -> bool {
    classify(name).is_some_and(|token| reserved_at(token, source, start, end))
}

fn reserved_at(token: FunctionToken, source: &str, start: usize, end: usize) -> bool {
    let tail = &source[end..];
    let after_dot = start > 0 && source.as_bytes()[start - 1] == b'.';
    let before_dot = tail.as_bytes().first() == Some(&b'.')
        && tail.as_bytes().get(1).is_some_and(|&byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$') || !byte.is_ascii()
        });
    !after_dot && !before_dot && token.reserved_as_identifier(call_follows(tail))
}

fn call_follows(tail: &str) -> bool {
    if !ignore_space() {
        return tail.starts_with('(');
    }
    let tail = tail.trim_start_matches(|ch: char| ch.is_ascii_whitespace());
    if tail.starts_with(['/', '-', '#']) {
        let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
        return crate::sql_lex::skip_trivia(tail, 0, mode, super::MYSQL_VERSION_ID, true)
            .is_ok_and(|offset| tail[offset..].starts_with('('));
    }
    tail.starts_with('(')
}

pub(crate) fn is_clock_precision_function(name: &str) -> bool {
    classify(name).is_some_and(FunctionToken::has_precision)
}

pub(super) fn validate_identifier(ident: &Ident) -> Result<(), ParseError> {
    if ident.value.contains('\0') {
        return parse_error_from_sql_span(
            ident.span,
            "NUL is not allowed in an SQL identifier",
            vec![],
        );
    }
    if ident.quote_style.is_some() {
        return Ok(());
    }
    let Some(token) = classify(&ident.value) else {
        return Ok(());
    };
    let reserved = raw_source_byte_span(ident.span)
        .and_then(|span| {
            with_parser_source(|source| reserved_at(token, source, span.start, span.end))
        })
        .unwrap_or_else(|| token.reserved_as_identifier(false));
    if reserved {
        return parse_error_from_sql_span(
            ident.span,
            format!("reserved identifier {} must be quoted", ident.value),
            vec![],
        );
    }
    Ok(())
}

pub(super) fn parse_prefix(parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
    let head = parser.peek_token_ref();
    let Token::Word(word) = &head.token else {
        return None;
    };
    let next = parser.peek_nth_token_ref(1);
    let has_parens = next.token == Token::LParen;
    let quoted = word.quote_style.is_some();
    if !has_parens && (quoted || !can_be_keyword_function(&word.value)) {
        return None;
    }
    let token = classify(&word.value)?;
    let previous = (0..=parser.get_current_index())
        .rev()
        .map(|index| parser.token_at(index))
        .find(|token| !matches!(token.token, Token::Whitespace(_)));
    let dot = previous.filter(|token| token.token == Token::Period);
    let before_dot = next.token == Token::Period
        && head.span.end == next.span.start
        && matches!(parser.peek_nth_token_ref(2), TokenWithSpan { token: Token::Word(part), span }
            if part.quote_style.is_none() && next.span.end == span.start);
    if dot.is_some() || before_dot {
        // MySQL forces identifier tokenization when an unquoted component
        // touches a dot on its left, or is immediately followed by .word.
        let keyword_lookup =
            !quoted && !before_dot && dot.is_some_and(|dot| dot.span.end != head.span.start);
        if keyword_lookup
            && token.reserved_as_identifier(
                has_parens && (head.span.end == next.span.start || ignore_space()),
            )
        {
            return Some(Err(ParserError::ParserError(format!(
                "expected an identifier after '.' at Line: {}, Column: {}",
                head.span.start.line, head.span.start.column,
            ))));
        }
        let ident = Ident {
            value: word.value.clone(),
            quote_style: word.quote_style,
            span: head.span,
        };
        parser.next_token();
        return Some(if has_parens {
            parser.parse_function(ident.into())
        } else {
            Ok(Expr::Identifier(ident))
        });
    }
    if quoted
        || (matches!(token, FunctionToken::Adjacent { .. })
            && has_parens
            && head.span.end != next.span.start
            && !ignore_space())
    {
        if !has_parens {
            return None;
        }
        // Use quote provenance in the upstream AST for both generic spellings.
        let ident = Ident::with_quote_and_span('`', head.span, word.value.clone());
        parser.next_token();
        return Some(parse_generic_call(parser, ident));
    }
    let precision = match token {
        FunctionToken::Keyword {
            precision,
            optional_parentheses,
        } => {
            if !optional_parentheses && !has_parens {
                return Some(Err(ParserError::ParserError(format!(
                    "{} requires parentheses at Line: {}, Column: {}",
                    word.value, head.span.start.line, head.span.start.column
                ))));
            }
            precision
        }
        FunctionToken::Adjacent {
            precision: Some(precision),
            ..
        } if has_parens => precision,
        _ => return None,
    };
    let ident = Ident {
        value: word.value.clone(),
        quote_style: word.quote_style,
        span: head.span,
    };
    parser.next_token();
    Some(parse_clock_call(parser, ident, precision))
}

fn parse_clock_call(
    parser: &mut Parser,
    ident: Ident,
    precision: bool,
) -> Result<Expr, ParserError> {
    let mut args = Vec::new();
    if parser.consume_token(&Token::LParen) && !parser.consume_token(&Token::RParen) {
        if !precision {
            parser.expect_token(&Token::RParen)?;
        }
        let number = parser.next_token();
        let Token::Number(text, false) = &number.token else {
            return Err(expected_precision(&number));
        };
        let Some(value) = text
            .bytes()
            .all(|byte| byte.is_ascii_digit())
            .then(|| text.parse::<i32>().ok())
            .flatten()
        else {
            return Err(expected_precision(&number));
        };
        // MySQL's grammar accepts a NUM token and narrows it to uint8 before
        // rejecting an out-of-range fractional precision.
        let value = sqlparser::ast::Value::Number((value as u8).to_string(), false);
        args.push(Expr::Value(value.with_span(number.span)));
        parser.expect_token(&Token::RParen)?;
    }
    Ok(super::adapter::scalar_function(ident, args))
}

fn expected_precision(token: &TokenWithSpan) -> ParserError {
    ParserError::ParserError(format!(
        "expected an unsigned integer precision at Line: {}, Column: {}",
        token.span.start.line, token.span.start.column
    ))
}

/// A generic call uses expression arguments rather than native special syntax
/// such as COUNT(*) or EXTRACT(unit FROM expr). MySQL parses argument aliases
/// for UDFs and diagnoses them when resolving a stored function.
fn parse_generic_call(parser: &mut Parser, ident: Ident) -> Result<Expr, ParserError> {
    parser.expect_token(&Token::LParen)?;
    let mut call = super::adapter::scalar_function(ident, Vec::new());
    let Expr::Function(function) = &mut call else {
        unreachable!()
    };
    let FunctionArguments::List(list) = &mut function.args else {
        unreachable!()
    };
    if !parser.consume_token(&Token::RParen) {
        loop {
            let arg = FunctionArgExpr::Expr(parser.parse_expr()?);
            list.args.push(if parser.parse_keyword(Keyword::AS) {
                FunctionArg::Named {
                    name: parser.parse_identifier()?,
                    arg,
                    operator: FunctionArgOperator::Equals,
                }
            } else {
                FunctionArg::Unnamed(arg)
            });
            if !parser.consume_token(&Token::Comma) {
                break;
            }
        }
        parser.expect_token(&Token::RParen)?;
    }
    Ok(call)
}

/// A leading dot records stored-only lookup in the scalar AST. The catalog's
/// name splitter binds a single component in the current database, and this
/// spelling cannot be produced by SQL object-name conversion.
pub(crate) fn stored_only_function(name: &str) -> Option<&str> {
    name.strip_prefix('.')
}

/// Column attributes only allow CURRENT_TIMESTAMP and its NOW aliases as
/// unparenthesized clock calls. Parenthesized DEFAULT expressions use the
/// ordinary expression grammar.
pub(super) fn validate_clock_column_option(expr: &Expr) -> Result<(), ParseError> {
    let ident = match expr {
        Expr::Function(function) => match function.name.0.as_slice() {
            [sqlparser::ast::ObjectNamePart::Identifier(ident)] => ident,
            _ => return Ok(()),
        },
        Expr::Identifier(ident) => ident,
        _ => return Ok(()),
    };
    if classify(&ident.value).is_some_and(|token| match token {
        FunctionToken::Keyword { .. }
        | FunctionToken::Adjacent {
            precision: Some(_), ..
        } => true,
        _ => false,
    }) && (!matches!(expr, Expr::Function(_))
        || ident.quote_style.is_some()
        || !is_local_timestamp_alias(&ident.value))
    {
        return parse_error_from_sql_span(
            ident.span,
            "expected CURRENT_TIMESTAMP or an equivalent column attribute",
            vec![],
        );
    }
    Ok(())
}

fn is_local_timestamp_alias(name: &str) -> bool {
    ["NOW", "CURRENT_TIMESTAMP", "LOCALTIME", "LOCALTIMESTAMP"]
        .iter()
        .any(|alias| name.eq_ignore_ascii_case(alias))
}

pub(super) fn scalar_call_name(
    function: &sqlparser::ast::Function,
    mut name: String,
) -> Result<String, ParseError> {
    let [sqlparser::ast::ObjectNamePart::Identifier(ident)] = function.name.0.as_slice() else {
        return Ok(name);
    };
    let Some(token) = classify(&name) else {
        return Ok(name);
    };
    if ident.quote_style.is_some() {
        if matches!(&function.args, FunctionArguments::List(list) if list.args.iter().any(|arg| matches!(arg, FunctionArg::Named { .. })))
        {
            return Err(super::parse_error_with_spec_at_zero(
                crate::error::catalog::ER_WRONG_PARAMETERS_TO_STORED_FCT,
                format!("Incorrect parameters in the call to stored function {name}"),
            ));
        }
        name.insert(0, '.');
    } else if token.has_precision() {
        if let FunctionArguments::List(list) = &function.args {
            if let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value)))] =
                list.args.as_slice()
            {
                if let sqlparser::ast::Value::Number(text, false) = &value.value {
                    if let Ok(fsp @ 7..) = text.parse::<u8>() {
                        let canonical = if is_local_timestamp_alias(&name) {
                            "now"
                        } else if name.eq_ignore_ascii_case("CURRENT_TIME") {
                            "curtime"
                        } else {
                            &name
                        }
                        .to_ascii_lowercase();
                        let mut error = super::parse_error_at_span(
                            value.span,
                            format!("Too-big precision {fsp} specified for '{canonical}'. Maximum is 6."),
                        );
                        error.spec = crate::error::catalog::ER_TOO_BIG_PRECISION;
                        return Err(error);
                    }
                }
            }
        }
    }
    Ok(name)
}
