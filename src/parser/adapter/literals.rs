//! Normalize lexical literal spellings without editing source text or spans.
use sqlparser::{
    keywords::Keyword,
    tokenizer::{Token, TokenWithSpan},
};

pub(in crate::parser) fn concatenate(tokens: &mut Vec<TokenWithSpan>) {
    let mut read = 0;
    let mut write = 0;
    while read < tokens.len() {
        if write != read {
            tokens.swap(write, read);
        }
        read += 1;
        if matches!(
            tokens[write].token,
            Token::SingleQuotedString(_)
                | Token::DoubleQuotedString(_)
                | Token::NationalStringLiteral(_)
        ) {
            loop {
                let mut next = read;
                while next < tokens.len() && matches!(tokens[next].token, Token::Whitespace(_)) {
                    next += 1;
                }
                let suffix = match tokens.get_mut(next).map(|token| &mut token.token) {
                    Some(Token::SingleQuotedString(value) | Token::DoubleQuotedString(value)) => {
                        std::mem::take(value)
                    }
                    _ => break,
                };
                let end = tokens[next].span.end;
                match &mut tokens[write].token {
                    Token::SingleQuotedString(value)
                    | Token::DoubleQuotedString(value)
                    | Token::NationalStringLiteral(value) => value.push_str(&suffix),
                    _ => unreachable!(),
                }
                tokens[write].span.end = end;
                read = next + 1;
            }
        }
        write += 1;
    }
    tokens.truncate(write);
}

pub(in crate::parser) fn may_contain(sql: &str) -> bool {
    memchr::memchr_iter(b'0', sql.as_bytes())
        .any(|start| matches!(sql.as_bytes().get(start + 1), Some(b'b' | b'B')))
}

/// Invalid 0x-prefixed numeric spellings are digit-leading identifiers in
/// MySQL. Change only their first ASCII byte while tokenizing, then restore
/// the exact original word. This also preserves adjacent quoted aliases:
/// 0xX'alias' is an identifier followed by a string, not two hex literals.
pub(super) fn tokenize(
    dialect: &dyn sqlparser::dialect::Dialect,
    sql: &str,
) -> Result<Vec<TokenWithSpan>, sqlparser::tokenizer::TokenizerError> {
    let identifier_byte =
        |byte: &u8| byte.is_ascii_alphanumeric() || !byte.is_ascii() || matches!(byte, b'_' | b'$');
    let suspicious = memchr::memchr_iter(b'0', sql.as_bytes()).any(|start| {
        if !matches!(sql.as_bytes().get(start + 1), Some(b'x' | b'X')) {
            return false;
        }
        let tail = &sql.as_bytes()[start + 2..];
        let len = tail.iter().take_while(|byte| identifier_byte(byte)).count();
        len == 0 || !tail[..len].iter().all(u8::is_ascii_hexdigit)
    });
    if !suspicious {
        return sqlparser::tokenizer::Tokenizer::new(dialect, sql).tokenize_with_location();
    }
    let mode =
        crate::sql_lex::SqlLexMode::from_parser_bits(crate::parser::current_parser_sql_mode_bits());
    let mut cursor =
        crate::sql_lex::SqlTokenCursor::new(sql, mode, crate::parser::MYSQL_VERSION_ID);
    let mut starts = Vec::new();
    while let Ok(Some(token)) = cursor.next() {
        if token.kind != crate::sql_lex::SqlTokenKind::Number {
            continue;
        }
        let start = token.span.start;
        if !sql
            .as_bytes()
            .get(start..start + 2)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"0x"))
        {
            continue;
        }
        let tail = &sql.as_bytes()[start + 2..];
        let len = tail.iter().take_while(|byte| identifier_byte(byte)).count();
        if len == 0 || !tail[..len].iter().all(u8::is_ascii_hexdigit) {
            starts.push(start);
        }
    }
    if starts.is_empty() {
        return sqlparser::tokenizer::Tokenizer::new(dialect, sql).tokenize_with_location();
    }
    let mut masked = sql.as_bytes().to_vec();
    for start in &starts {
        masked[*start] = b'q';
    }
    let masked = String::from_utf8(masked).expect("ASCII substitution preserves UTF-8");
    let mut tokens =
        sqlparser::tokenizer::Tokenizer::new(dialect, &masked).tokenize_with_location()?;
    let mut position = super::super::source::SourcePosition::start();
    let mut starts = starts.into_iter().peekable();
    for token in &mut tokens {
        let Some(next) = starts.peek().copied() else {
            break;
        };
        let start = position
            .seek(sql, token.span.start.line, token.span.start.column)
            .unwrap_or(sql.len());
        let end = position
            .seek(sql, token.span.end.line, token.span.end.column)
            .unwrap_or(sql.len());
        if next >= start && next < end {
            if let Token::Word(word) = &mut token.token {
                word.value = sql[start..end].to_owned();
                word.keyword = Keyword::NoKeyword;
            }
            while starts.peek().is_some_and(|next| *next < end) {
                starts.next();
            }
        }
    }
    Ok(tokens)
}

pub(in crate::parser) fn normalize(tokens: &mut [TokenWithSpan]) {
    for token in tokens {
        let Token::Word(word) = &mut token.token else {
            continue;
        };
        let bytes = word.value.as_bytes();
        if word.quote_style.is_none()
            && bytes.len() > 2
            && bytes[0] == b'0'
            && matches!(bytes[1], b'b' | b'B')
            && bytes[2..].iter().all(|byte| matches!(byte, b'0' | b'1'))
        {
            word.value.drain(..2);
            token.token = Token::SingleQuotedByteStringLiteral(std::mem::take(&mut word.value));
        }
    }
}
