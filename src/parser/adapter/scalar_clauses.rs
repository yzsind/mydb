//! Scalar-function clauses consume the existing token stream. Nested calls
//! keep their AST and source spans instead of being rendered and reparsed.
use super::scalar_function;
use sqlparser::{
    ast::{Expr, Value},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(in crate::parser) fn parse_prefix(parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
    let Token::Word(word) = &parser.peek_token_ref().token else {
        return None;
    };
    if word.quote_style.is_some() || parser.peek_nth_token_ref(1).token != Token::LParen {
        return None;
    }
    if word.value.eq_ignore_ascii_case("CHAR") {
        Some(parse_char(parser))
    } else if word.value.eq_ignore_ascii_case("WEIGHT_STRING") {
        Some(parse_weight_string(parser))
    } else if word.value.eq_ignore_ascii_case("EXTRACT")
        && matches!(&parser.peek_nth_token_ref(2).token, Token::Word(unit)
            if unit.quote_style.is_none() && compound_unit(&unit.value))
    {
        Some(parse_extract(parser))
    } else {
        None
    }
}

fn string(value: String) -> Expr {
    Expr::Value(Value::SingleQuotedString(value).into())
}

fn parse_char(parser: &mut Parser) -> Result<Expr, ParserError> {
    let name = parser.parse_identifier()?;
    parser.expect_token(&Token::LParen)?;
    if parser.consume_token(&Token::RParen) {
        return Ok(scalar_function(name, Vec::new()));
    }
    let args = parser.parse_comma_separated(Parser::parse_expr)?;
    if parser.consume_token(&Token::RParen) {
        return Ok(scalar_function(name, args));
    }
    parser.expect_keyword(Keyword::USING)?;
    let charset = parser.parse_identifier()?;
    parser.expect_token(&Token::RParen)?;
    let inner = scalar_function(name.clone(), args);
    let mut using = name;
    using.value = "CHAR_USING".into();
    Ok(scalar_function(using, vec![inner, string(charset.value)]))
}

fn parse_weight_string(parser: &mut Parser) -> Result<Expr, ParserError> {
    let mut name = parser.parse_identifier()?;
    parser.expect_token(&Token::LParen)?;
    if parser.consume_token(&Token::RParen) {
        return Ok(scalar_function(name, Vec::new()));
    }
    let mut args = parser.parse_comma_separated(Parser::parse_expr)?;
    if parser.consume_token(&Token::RParen) {
        return Ok(scalar_function(name, args));
    }
    if args.len() != 1 {
        return parser.expected("one WEIGHT_STRING value before AS", parser.peek_token());
    }
    parser.expect_keyword(Keyword::AS)?;
    let kind = if parser.parse_keyword(Keyword::CHAR) {
        "CHAR"
    } else if parser.parse_keyword(Keyword::BINARY) {
        "BINARY"
    } else {
        return parser.expected("CHAR or BINARY", parser.peek_token());
    };
    parser.expect_token(&Token::LParen)?;
    let token = parser.next_token();
    let length = match token.token {
        Token::Number(ref number, long)
            if !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            Expr::Value(Value::Number(number.clone(), long).with_span(token.span))
        }
        _ => return parser.expected("unsigned integer length", token),
    };
    parser.expect_token(&Token::RParen)?;
    parser.expect_token(&Token::RParen)?;
    args.extend([string(kind.into()), length]);
    name.value = "WEIGHT_STRING_TYPED".into();
    Ok(scalar_function(name, args))
}

fn compound_unit(unit: &str) -> bool {
    const UNITS: &[&str] = &[
        "YEAR_MONTH",
        "DAY_HOUR",
        "DAY_MINUTE",
        "DAY_SECOND",
        "DAY_MICROSECOND",
        "HOUR_MINUTE",
        "HOUR_SECOND",
        "HOUR_MICROSECOND",
        "MINUTE_SECOND",
        "MINUTE_MICROSECOND",
        "SECOND_MICROSECOND",
    ];
    UNITS.iter().any(|known| unit.eq_ignore_ascii_case(known))
}

fn parse_extract(parser: &mut Parser) -> Result<Expr, ParserError> {
    let mut name = parser.parse_identifier()?;
    name.value.make_ascii_uppercase();
    parser.expect_token(&Token::LParen)?;
    let mut unit = parser.parse_identifier()?;
    unit.value.make_ascii_uppercase();
    parser.expect_keyword(Keyword::FROM)?;
    let expr = parser.parse_expr()?;
    parser.expect_token(&Token::RParen)?;
    // The upstream Extract node drops the function's source span. A scalar
    // call keeps the original name span and lowers to the same mydb EXTRACT
    // expression, without a synthetic EXTRACT_COMPOUND spelling.
    Ok(scalar_function(name, vec![string(unit.value), expr]))
}
