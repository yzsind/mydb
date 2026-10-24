//! Column-type extensions missing from upstream's type parser. The dialect
//! transports them as its designated extension tokens; conversion receives
//! typed suffixes, never rewritten SQL or out-of-band parser state.
use super::super::{parse_error_at_zero, ParseError};
use sqlparser::{
    ast::{CharacterLength, ColumnOption, ExactNumberInfo},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(in crate::parser) fn parse_suffix(
    parser: &mut Parser,
) -> Option<Result<ColumnOption, ParserError>> {
    if matches!(&parser.peek_token_ref().token, Token::Word(word)
        if word.quote_style.is_none() && matches!(word.keyword, Keyword::CHAR | Keyword::CHARACTER | Keyword::VARCHAR | Keyword::VARYING))
        && !matches!(&parser.peek_nth_token_ref(1).token, Token::Word(word) if word.keyword == Keyword::SET)
    {
        return Some((|| {
            let token = parser.next_token().token;
            let mut tokens = vec![token];
            if matches!(&tokens[0], Token::Word(word) if matches!(word.keyword, Keyword::CHAR | Keyword::CHARACTER))
                && matches!(&parser.peek_token_ref().token, Token::Word(word) if word.keyword == Keyword::VARYING)
            {
                tokens.push(parser.next_token().token);
            }
            let length = parser.parse_optional_character_length()?;
            if let Some(CharacterLength::IntegerLength { length, .. }) = length {
                tokens.push(Token::Number(length.to_string(), false));
            }
            Ok(ColumnOption::DialectSpecific(tokens))
        })());
    }

    if parser.peek_token_ref().token == Token::LParen {
        return Some(
            parser
                .parse_exact_number_optional_precision_scale()
                .map(|info| {
                    let mut tokens = vec![Token::LParen];
                    match info {
                        ExactNumberInfo::Precision(precision) => {
                            tokens.push(Token::Number(precision.to_string(), false))
                        }
                        ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                            tokens.push(Token::Number(precision.to_string(), false));
                            tokens.push(Token::Comma);
                            tokens.push(Token::Number(scale.to_string(), false));
                        }
                        ExactNumberInfo::None => unreachable!("opening parenthesis checked"),
                    }
                    tokens.push(Token::RParen);
                    ColumnOption::DialectSpecific(tokens)
                }),
        );
    }
    if matches!(&parser.peek_token_ref().token, Token::Word(word)
        if word.quote_style.is_none() && ["BYTE", "SIGNED", "UNSIGNED", "ZEROFILL"].iter().any(|name| word.value.eq_ignore_ascii_case(name)))
    {
        return Some(Ok(ColumnOption::DialectSpecific(vec![
            parser.next_token().token,
        ])));
    }
    None
}

pub(in crate::parser) enum TypeSuffix {
    National {
        initial: Keyword,
        varying: bool,
        length: Option<u64>,
    },
    Zerofill,
    Byte,
    Signed,
    Unsigned,
    Precision(ExactNumberInfo),
}

pub(in crate::parser) fn decode_suffix(tokens: &[Token]) -> Result<Option<TypeSuffix>, ParseError> {
    if let Some(Token::Word(word)) = tokens.first() {
        if matches!(
            word.keyword,
            Keyword::CHAR | Keyword::CHARACTER | Keyword::VARCHAR | Keyword::VARYING
        ) {
            let (varying, tail) = match &tokens[1..] {
                [Token::Word(next), rest @ ..]
                    if next.keyword == Keyword::VARYING
                        && matches!(word.keyword, Keyword::CHAR | Keyword::CHARACTER) =>
                {
                    (true, rest)
                }
                rest => (
                    matches!(word.keyword, Keyword::VARCHAR | Keyword::VARYING),
                    rest,
                ),
            };
            let length = match tail {
                [] => None,
                [Token::Number(value, false)] => Some(
                    value
                        .parse()
                        .map_err(|_| parse_error_at_zero("invalid national character length"))?,
                ),
                _ => return Err(parse_error_at_zero("invalid national character type")),
            };
            return Ok(Some(TypeSuffix::National {
                initial: word.keyword,
                varying,
                length,
            }));
        }
    }

    if let [Token::Word(word)] = tokens {
        return Ok(if word.value.eq_ignore_ascii_case("ZEROFILL") {
            Some(TypeSuffix::Zerofill)
        } else if word.value.eq_ignore_ascii_case("BYTE") {
            Some(TypeSuffix::Byte)
        } else if word.value.eq_ignore_ascii_case("SIGNED") {
            Some(TypeSuffix::Signed)
        } else if word.value.eq_ignore_ascii_case("UNSIGNED") {
            Some(TypeSuffix::Unsigned)
        } else {
            None
        });
    }
    if tokens.first() != Some(&Token::LParen) {
        return Ok(None);
    }
    let invalid = || parse_error_at_zero("invalid column type precision");
    let info = match tokens {
        [Token::LParen, Token::Number(p, false), Token::RParen] => {
            ExactNumberInfo::Precision(p.parse().map_err(|_| invalid())?)
        }
        [Token::LParen, Token::Number(p, false), Token::Comma, Token::Number(s, false), Token::RParen] => {
            ExactNumberInfo::PrecisionAndScale(
                p.parse().map_err(|_| invalid())?,
                s.parse().map_err(|_| invalid())?,
            )
        }
        _ => return Err(invalid()),
    };
    Ok(Some(TypeSuffix::Precision(info)))
}
