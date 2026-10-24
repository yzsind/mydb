//! MySQL TRIM directions and default removal strings are grammar, not edits
//! to the SQL buffer. Keep the upstream typed node for canonical display.
use sqlparser::{
    ast::{Expr, TrimWhereField, Value},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(in crate::parser) fn parse_prefix(parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
    if !matches!(&parser.peek_token_ref().token, Token::Word(word) if word.quote_style.is_none() && word.keyword == Keyword::TRIM)
        || parser.peek_nth_token_ref(1).token != Token::LParen
    {
        return None;
    }
    Some(parse_trim(parser))
}
fn parse_trim(parser: &mut Parser) -> Result<Expr, ParserError> {
    let name = parser.parse_identifier()?;
    parser.expect_token(&Token::LParen)?;
    if parser.peek_token_ref().token == Token::RParen {
        return parser.expected("TRIM expression", parser.peek_token());
    }
    let direction = if parser.parse_keyword(Keyword::LEADING) {
        Some(TrimWhereField::Leading)
    } else if parser.parse_keyword(Keyword::TRAILING) {
        Some(TrimWhereField::Trailing)
    } else if parser.parse_keyword(Keyword::BOTH) {
        Some(TrimWhereField::Both)
    } else {
        None
    };
    let (value, removal) = if direction.is_some() {
        let removal = if parser.parse_keyword(Keyword::FROM) {
            Expr::Value(Value::SingleQuotedString(" ".into()).into())
        } else {
            let removal = parser.parse_expr()?;
            parser.expect_keyword(Keyword::FROM)?;
            removal
        };
        (parser.parse_expr()?, Some(Box::new(removal)))
    } else {
        let first = parser.parse_expr()?;
        if parser.parse_keyword(Keyword::FROM) {
            (parser.parse_expr()?, Some(Box::new(first)))
        } else if parser.consume_token(&Token::Comma) {
            // Preserve the scalar-call form used by persisted expression SQL.
            let mut args = vec![first];
            args.extend(parser.parse_comma_separated(Parser::parse_expr)?);
            parser.expect_token(&Token::RParen)?;
            return Ok(super::scalar_function(name, args));
        } else {
            (first, None)
        }
    };
    parser.expect_token(&Token::RParen)?;
    Ok(Expr::Trim {
        expr: Box::new(value),
        trim_where: direction,
        trim_what: removal,
        trim_characters: None,
    })
}
