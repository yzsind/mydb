//! MySQL expression casts are parsed in expression context, independently of
//! BINARY column types and charset names in DDL. No SQL text is rewritten.
use sqlparser::{
    ast::{CastKind, DataType, Expr},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(in crate::parser) fn parse_prefix(
    parser: &mut Parser,
    binary_precedence: u8,
) -> Option<Result<Expr, ParserError>> {
    let Token::Word(word) = &parser.peek_token_ref().token else {
        return None;
    };
    if word.quote_style.is_some() {
        return None;
    }
    match word.keyword {
        Keyword::BINARY => {
            let _ = parser.next_token();
            Some(
                parser
                    .parse_subexpr(binary_precedence)
                    .map(|expr| cast(expr, DataType::Binary(None), false, None)),
            )
        }
        Keyword::CAST if parser.peek_nth_token_ref(1).token == Token::LParen => {
            Some(parse_cast(parser))
        }
        _ => None,
    }
}

fn cast(
    expr: Expr,
    data_type: DataType,
    array: bool,
    format: Option<sqlparser::ast::CastFormat>,
) -> Expr {
    Expr::Cast {
        kind: CastKind::Cast,
        expr: Box::new(expr),
        data_type,
        array,
        format,
    }
}

fn parse_cast(parser: &mut Parser) -> Result<Expr, ParserError> {
    let name = parser.parse_identifier()?;
    parser.expect_token(&Token::LParen)?;
    if parser.consume_token(&Token::RParen) {
        return Ok(super::scalar_function(name, Vec::new()));
    }
    let expr = parser.parse_expr()?;
    // Retain the scalar-call representation used by existing catalog SQL.
    // Consume it directly, rather than reparsing after a failed AS attempt.
    if parser.consume_token(&Token::Comma) {
        let mut args = vec![expr];
        args.extend(parser.parse_comma_separated(Parser::parse_expr)?);
        parser.expect_token(&Token::RParen)?;
        return Ok(super::scalar_function(name, args));
    }
    if parser.consume_token(&Token::RParen) {
        return Ok(super::scalar_function(name, vec![expr]));
    }
    parser.expect_keyword(Keyword::AS)?;
    let data_type = parser.parse_data_type()?;
    if matches!(data_type, DataType::Signed | DataType::Unsigned) {
        let _ = parser.parse_keyword(Keyword::INT);
    }
    let charset = if parser.parse_keyword(Keyword::CHARSET) {
        Some(parser.parse_identifier()?)
    } else if parser.parse_keyword(Keyword::CHARACTER) {
        parser.expect_keyword(Keyword::SET)?;
        Some(parser.parse_identifier()?)
    } else {
        None
    };
    if charset.is_some() && !matches!(data_type, DataType::Char(_) | DataType::Character(_)) {
        return parser.expected("CHAR cast target before CHARACTER SET", parser.peek_token());
    }
    let array = parser.parse_keyword(Keyword::ARRAY);
    let format = parser.parse_optional_cast_format()?;
    parser.expect_token(&Token::RParen)?;
    let expr = cast(expr, data_type, array, format);
    Ok(match charset {
        Some(charset) => Expr::Convert {
            is_try: false,
            expr: Box::new(expr),
            data_type: None,
            charset: Some(charset.into()),
            target_before_value: false,
            styles: Vec::new(),
        },
        None => expr,
    })
}
