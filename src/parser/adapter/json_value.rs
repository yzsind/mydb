//! Native JSON_VALUE clause grammar. Expressions retain their original spans;
//! lowering builds arguments directly instead of rendering and reparsing SQL.
use super::scalar_function as call;
use sqlparser::{
    ast::{Expr, Value},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(in crate::parser) fn parse_prefix(parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
    let token = parser.peek_token_ref();
    if !matches!(&token.token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("JSON_VALUE"))
        || parser.peek_nth_token_ref(1).token != Token::LParen
    {
        return None;
    }
    Some(parse_call(parser))
}

fn string(value: impl Into<String>) -> Expr {
    Expr::Value(Value::SingleQuotedString(value.into()).into())
}
fn null() -> Expr {
    Expr::Value(Value::Null.into())
}
fn parse_call(parser: &mut Parser) -> Result<Expr, ParserError> {
    let name = parser.parse_identifier()?;
    parser.expect_token(&Token::LParen)?;
    if parser.consume_token(&Token::RParen) {
        return Ok(call(name, Vec::new()));
    }
    let mut args = parser.parse_comma_separated(Parser::parse_expr)?;
    if parser.consume_token(&Token::RParen) {
        // Preserve ordinary calls and the seven-argument catalog form. The
        // evaluator remains responsible for ordinary function arity errors.
        return Ok(call(name, args));
    }
    if args.len() != 2 {
        return parser.expected(
            "two JSON_VALUE arguments before response clauses",
            parser.peek_token(),
        );
    }
    let returning = if parser.parse_keyword(Keyword::RETURNING) {
        parse_returning_type(parser)?
    } else {
        "CHAR".to_owned()
    };
    let mut on_empty = None;
    let mut on_error = None;
    while !parser.consume_token(&Token::RParen) {
        let (policy, default) = if parser.parse_keyword(Keyword::NULL) {
            ("NULL", null())
        } else if parser.parse_keyword(Keyword::ERROR) {
            ("ERROR", null())
        } else if parser.parse_keyword(Keyword::DEFAULT) {
            ("DEFAULT", parser.parse_expr()?)
        } else {
            return parser.expected(
                "NULL, ERROR or DEFAULT in JSON_VALUE response clause",
                parser.peek_token(),
            );
        };
        parser.expect_keyword(Keyword::ON)?;
        let slot = if parser.parse_keyword(Keyword::EMPTY) {
            &mut on_empty
        } else if parser.parse_keyword(Keyword::ERROR) {
            &mut on_error
        } else {
            return parser.expected("EMPTY or ERROR after ON", parser.peek_token());
        };
        if slot.is_some() {
            return parser.expected(
                "a single response clause per JSON_VALUE condition",
                parser.peek_token(),
            );
        }
        *slot = Some((policy, default));
    }
    let (empty_policy, empty_default) = on_empty.unwrap_or_else(|| ("NULL", null()));
    let (error_policy, error_default) = on_error.unwrap_or_else(|| ("NULL", null()));
    args.extend([
        string(returning),
        string(empty_policy),
        empty_default,
        string(error_policy),
        error_default,
    ]);
    Ok(call(name, args))
}

fn parse_returning_type(parser: &mut Parser) -> Result<String, ParserError> {
    let token = parser.next_token();
    let Token::Word(word) = &token.token else {
        return parser.expected("JSON_VALUE RETURNING type", token);
    };
    let mut name = word.value.to_ascii_uppercase();
    if word.quote_style.is_some()
        || !matches!(
            name.as_str(),
            "FLOAT"
                | "DOUBLE"
                | "REAL"
                | "JSON"
                | "DECIMAL"
                | "SIGNED"
                | "UNSIGNED"
                | "CHAR"
                | "DATE"
                | "TIME"
                | "DATETIME"
                | "YEAR"
        )
    {
        return parser.expected("supported JSON_VALUE RETURNING type", token);
    }
    if matches!(name.as_str(), "SIGNED" | "UNSIGNED") {
        let _ = parser.parse_keyword(Keyword::INTEGER);
    }
    if parser.consume_token(&Token::LParen) {
        let precision = parser.parse_literal_uint()?;
        name.push('(');
        name.push_str(&precision.to_string());
        if parser.consume_token(&Token::Comma) {
            name.push(',');
            name.push_str(&parser.parse_literal_uint()?.to_string());
        }
        parser.expect_token(&Token::RParen)?;
        name.push(')');
    }
    Ok(name)
}
