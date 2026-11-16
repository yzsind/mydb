//! MySQL INTERVAL expressions and the INTERVAL(n, ...) scalar function share
//! a token entry point, but keep distinct AST forms and operand boundaries.
use super::scalar_function;
use sqlparser::{
    ast::{DateTimeField, Expr, Interval},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(in crate::parser) fn parse_prefix(parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
    if !matches!(&parser.peek_token_ref().token, Token::Word(word)
        if word.quote_style.is_none() && word.keyword == Keyword::INTERVAL)
    {
        return None;
    }
    Some(parse_interval(parser))
}

fn parse_interval(parser: &mut Parser) -> Result<Expr, ParserError> {
    let mut name = parser.parse_identifier()?;
    let value = if parser.peek_token_ref().token == Token::LParen {
        let prefix = parser.parse_prefix()?;
        if let Expr::Tuple(args) = prefix {
            name.value = "__INTERVAL_LIST".into();
            return Ok(scalar_function(name, args));
        }
        // A parenthesized interval value can continue with arithmetic before
        // its unit. Continue the already-parsed prefix; do not parse it twice.
        let mut value = parser.parse_compound_expr(prefix, Vec::new())?;
        loop {
            let precedence = parser.get_next_precedence()?;
            if precedence == 0 || parser.peek_token_ref().token == Token::Period {
                break;
            }
            value = parser.parse_infix(value, precedence)?;
        }
        value
    } else {
        parser.parse_expr()?
    };
    let token = parser.peek_token_ref();
    if !matches!(&token.token, Token::Word(word) if word.quote_style.is_none() && valid_unit(&word.value))
    {
        return parser.expected_ref("an explicit MySQL INTERVAL time unit", token);
    }
    let mut unit = parser.parse_identifier()?;
    unit.value.make_ascii_uppercase();
    Ok(Expr::Interval(Interval {
        value: Box::new(value),
        leading_field: Some(DateTimeField::Custom(unit)),
        leading_precision: None,
        last_field: None,
        fractional_seconds_precision: None,
    }))
}

fn valid_unit(unit: &str) -> bool {
    const UNITS: &[&str] = &[
        "MICROSECOND",
        "SECOND",
        "MINUTE",
        "HOUR",
        "DAY",
        "WEEK",
        "MONTH",
        "QUARTER",
        "YEAR",
        "SECOND_MICROSECOND",
        "MINUTE_MICROSECOND",
        "MINUTE_SECOND",
        "HOUR_MICROSECOND",
        "HOUR_SECOND",
        "HOUR_MINUTE",
        "DAY_MICROSECOND",
        "DAY_SECOND",
        "DAY_MINUTE",
        "DAY_HOUR",
        "YEAR_MONTH",
    ];
    UNITS.iter().any(|known| unit.eq_ignore_ascii_case(known))
}
