//! Pattern operators stay in the native token/AST stream. Dynamic
//! operands must not be removed and reparsed as independent SQL fragments.
use sqlparser::{
    ast::{BinaryOperator, Expr, Value},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};
pub(in crate::parser) const DYNAMIC_LIKE: &str = "LIKE ESCAPE";

pub(in crate::parser) fn parse_infix(
    parser: &mut Parser,
    left: &Expr,
    precedence: u8,
    escape_precedence: u8,
) -> Option<Result<Expr, ParserError>> {
    let negated =
        matches!(&parser.peek_token_ref().token, Token::Word(word) if word.keyword == Keyword::NOT);
    let operator = match &parser.peek_nth_token_ref(usize::from(negated)).token {
        Token::Word(word)
            if word.quote_style.is_none()
                && matches!(
                    word.keyword,
                    Keyword::LIKE | Keyword::REGEXP | Keyword::RLIKE
                ) =>
        {
            word.keyword
        }
        _ => return None,
    };
    Some((|| {
        if negated {
            parser.expect_keyword(Keyword::NOT)?;
        }
        parser.expect_keyword(operator)?;
        let pattern = parser.parse_subexpr(precedence)?;
        if operator != Keyword::LIKE {
            return Ok(Expr::RLike {
                expr: Box::new(left.clone()),
                pattern: Box::new(pattern),
                negated,
                regexp: operator == Keyword::REGEXP,
            });
        }
        let escape = if parser.parse_keyword(Keyword::ESCAPE) {
            Some(parser.parse_subexpr(escape_precedence)?)
        } else {
            None
        };
        let escape_char = match escape {
            None => None,
            Some(Expr::Value(value))
                if matches!(
                    value.value,
                    Value::SingleQuotedString(_)
                        | Value::DoubleQuotedString(_)
                        | Value::HexStringLiteral(_)
                        | Value::SingleQuotedByteStringLiteral(_)
                        | Value::DoubleQuotedByteStringLiteral(_)
                        | Value::NationalStringLiteral(_)
                ) =>
            {
                Some(value.value)
            }
            Some(escape) => {
                return Ok(Expr::BinaryOp {
                    left: Box::new(left.clone()),
                    op: BinaryOperator::Custom(DYNAMIC_LIKE.into()),
                    right: Box::new(Expr::Tuple(vec![
                        pattern,
                        escape,
                        Expr::Value(Value::Boolean(negated).into()),
                    ])),
                })
            }
        };
        Ok(Expr::Like {
            expr: Box::new(left.clone()),
            pattern: Box::new(pattern),
            negated,
            any: false,
            escape_char,
        })
    })())
}
