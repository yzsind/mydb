//! Normalize lexical literal spellings without editing source text or spans.
use sqlparser::{
    keywords::Keyword,
    tokenizer::{Token, TokenWithSpan},
};

pub(in crate::parser) fn concatenate(tokens: &mut Vec<TokenWithSpan>) {
    normalize_insert_parentheses(tokens);
    let mut read = 0;
    let mut write = 0;
    while read < tokens.len() {
        if write != read {
            tokens.swap(write, read);
        }
        read += 1;
        match &tokens[write].token {
            Token::Word(word) if word.keyword == Keyword::SELECT && word.quote_style.is_none() => {
                normalize_select_modifiers(&mut tokens[read..]);
            }
            Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::NationalStringLiteral(_) => loop {
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
            },
            _ => {}
        }
        write += 1;
    }
    tokens.truncate(write);
}

fn normalize_select_modifiers(tokens: &mut [TokenWithSpan]) {
    let mut distinct = None;
    for token in tokens {
        let Token::Word(word) = &token.token else {
            if matches!(token.token, Token::Whitespace(_)) {
                continue;
            }
            break;
        };
        if word.quote_style.is_some() {
            break;
        }
        let category = match word.keyword {
            Keyword::DISTINCT | Keyword::DISTINCTROW => Some(true),
            Keyword::ALL => Some(false),
            _ => None,
        };
        if let Some(category) = category {
            if distinct == Some(category) {
                token.token = Token::Whitespace(sqlparser::tokenizer::Whitespace::Space);
            } else {
                distinct = Some(category);
            }
        } else if !matches!(
            word.keyword,
            Keyword::HIGH_PRIORITY
                | Keyword::STRAIGHT_JOIN
                | Keyword::SQL_SMALL_RESULT
                | Keyword::SQL_BIG_RESULT
                | Keyword::SQL_BUFFER_RESULT
                | Keyword::SQL_NO_CACHE
                | Keyword::SQL_CALC_FOUND_ROWS
        ) {
            break;
        }
    }
}

fn normalize_insert_parentheses(tokens: &mut [TokenWithSpan]) {
    let significant = |token: &TokenWithSpan| !matches!(token.token, Token::Whitespace(_));
    if !matches!(tokens.iter().find(|t| significant(t)).map(|t| &t.token),
        Some(Token::Word(w)) if matches!(w.keyword, Keyword::INSERT | Keyword::REPLACE))
    {
        return;
    }
    let mut prefix = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| significant(t))
        .peekable();
    prefix.next();
    while prefix.peek().is_some_and(|(_, t)| {
        matches!(&t.token, Token::Word(w)
        if w.quote_style.is_none() && matches!(w.keyword,
            Keyword::IGNORE | Keyword::LOW_PRIORITY | Keyword::HIGH_PRIORITY | Keyword::DELAYED))
    }) {
        prefix.next();
    }
    if prefix.peek().is_some_and(|(_, t)| {
        matches!(&t.token, Token::Word(w)
        if w.quote_style.is_none() && w.keyword == Keyword::INTO)
    }) {
        prefix.next();
    }
    // Skip the target's identifier components before recognizing source clauses.
    if prefix.next().is_none() {
        return;
    }
    while prefix.peek().is_some_and(|(_, t)| t.token == Token::Period) {
        prefix.next();
        if prefix.next().is_none() {
            return;
        }
    }
    let Some(&(source_start, _)) = prefix.peek() else {
        return;
    };
    let mut depth = 0usize;
    let mut first_select = None;
    for (index, token) in tokens.iter().enumerate().skip(source_start) {
        match &token.token {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            Token::Word(w) if w.quote_style.is_none() => match w.keyword {
                Keyword::VALUES | Keyword::VALUE | Keyword::SET if depth == 0 => return,
                Keyword::SELECT => {
                    first_select = Some(index);
                    break;
                }
                _ => {}
            },
            _ => {}
        }
    }
    let Some(first_select) = first_select else {
        return;
    };
    if tokens[..first_select]
        .iter()
        .rev()
        .filter(|token| significant(token))
        .take_while(|token| token.token == Token::LParen)
        .take(2)
        .count()
        < 2
    {
        return;
    }
    let indices = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| significant(t))
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    let Some(select) = indices
        .iter()
        .position(|&i| matches!(&tokens[i].token, Token::Word(w) if w.keyword == Keyword::SELECT))
    else {
        return;
    };
    let start = (0..select)
        .rev()
        .take_while(|&p| tokens[indices[p]].token == Token::LParen)
        .last()
        .unwrap_or(select);
    if select - start < 2 {
        return;
    }
    let mut depth = 0usize;
    for position in select..indices.len() {
        match tokens[indices[position]].token {
            Token::LParen => depth += 1,
            Token::RParen if depth > 0 => depth -= 1,
            Token::RParen => {
                let count = select - start;
                if indices[position..]
                    .iter()
                    .take(count)
                    .all(|&i| tokens[i].token == Token::RParen)
                    && indices.len() - position >= count
                {
                    for p in start..select - 1 {
                        tokens[indices[p]].token =
                            Token::Whitespace(sqlparser::tokenizer::Whitespace::Space);
                    }
                    for p in position + 1..position + count {
                        tokens[indices[p]].token =
                            Token::Whitespace(sqlparser::tokenizer::Whitespace::Space);
                    }
                }
                break;
            }
            _ => {}
        }
    }
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
    let mut tokens = tokenize_hex_prefixes(dialect, sql)?;
    // A decimal point terminates MySQL's digit-leading identifier rule:
    // 1abc is a name, whereas 1.5abc is a number followed by an alias.
    // Split upstream Word tokens without rewriting SQL or losing spans.
    if may_contain_decimal_alias(sql, tokens.len()) {
        if let Some(start) = tokens
            .iter()
            .position(|token| decimal_alias_split(&token.token).is_some())
        {
            expand_decimal_alias_tail(&mut tokens, start);
        }
    }
    if !may_merge_numeric_identifiers(sql, tokens.len()) {
        return Ok(tokens);
    }
    let mut read = 0;
    let mut write = 0;
    while read < tokens.len() {
        let merged = match (&tokens[read].token, tokens.get(read + 1)) {
            (Token::Number(number, _), Some(next)) => match &next.token {
                Token::Word(word)
                    if word.quote_style.is_none()
                        && matches!(word.value.as_bytes().first(), Some(b'e' | b'E'))
                        && tokens[read].span.end == next.span.start
                        && !number.contains(['e', 'E']) =>
                {
                    if number.contains('.') {
                        return Err(sqlparser::tokenizer::TokenizerError {
                            message: "Invalid numeric literal with incomplete exponent".into(),
                            location: tokens[read].span.start,
                        });
                    }
                    let mut word = word.clone();
                    word.value = format!("{number}{}", word.value);
                    word.keyword = Keyword::NoKeyword;
                    Some((word, next.span.end))
                }
                _ => None,
            },
            _ => None,
        };
        if write != read {
            tokens.swap(write, read);
        }
        if let Some((word, end)) = merged {
            tokens[write].token = Token::Word(word);
            tokens[write].span.end = end;
            read += 2;
        } else {
            read += 1;
        }
        write += 1;
    }
    tokens.truncate(write);
    Ok(tokens)
}

fn may_merge_numeric_identifiers(sql: &str, budget: usize) -> bool {
    let bytes = sql.as_bytes();
    let mut candidates = memchr::memchr2_iter(b'e', b'E', bytes);
    // Bound raw lookahead by token count. Long string payloads must not
    // turn this fast path into a byte-by-byte scan.
    let mut checked = 0;
    while checked < budget {
        let Some(index) = candidates.next() else {
            return false;
        };
        if index > 0 && (bytes[index - 1].is_ascii_digit() || bytes[index - 1] == b'.') {
            return true;
        }
        checked += 1;
    }
    true
}

fn may_contain_decimal_alias(sql: &str, budget: usize) -> bool {
    let bytes = sql.as_bytes();
    let mut checked = 0;
    for point in memchr::memchr_iter(b'.', bytes) {
        if checked >= budget {
            return true;
        }
        checked += 1;
        let mut start = point;
        while start > 0 && bytes[start - 1].is_ascii_digit() {
            if checked >= budget {
                return true;
            }
            checked += 1;
            start -= 1;
        }
        if start == point && !bytes.get(point + 1).is_some_and(u8::is_ascii_digit) {
            continue;
        }
        let mut end = point + 1;
        while bytes.get(end).is_some_and(u8::is_ascii_digit) {
            if checked >= budget {
                return true;
            }
            checked += 1;
            end += 1;
        }
        if !bytes.get(end).is_some_and(|byte| {
            byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$') || !byte.is_ascii()
        }) {
            continue;
        }
        // Non-ASCII boundaries may be Unicode whitespace or identifier bytes.
        if start == 0
            || !(bytes[start - 1].is_ascii_alphanumeric()
                || matches!(bytes[start - 1], b'_' | b'$'))
        {
            return true;
        }
    }
    false
}

#[inline(always)]
fn decimal_alias_split(token: &Token) -> Option<usize> {
    let Token::Word(word) = token else {
        return None;
    };
    if word.quote_style.is_some()
        || !matches!(word.value.as_bytes().first(), Some(b'0'..=b'9' | b'.'))
    {
        return None;
    }
    let bytes = word.value.as_bytes();
    let integer = bytes.iter().take_while(|b| b.is_ascii_digit()).count();
    if bytes.get(integer) != Some(&b'.') {
        return None;
    }
    let end = integer
        + 1
        + bytes[integer + 1..]
            .iter()
            .take_while(|b| b.is_ascii_digit())
            .count();
    (end < bytes.len() && (integer > 0 || end > 1)).then_some(end)
}

fn expand_decimal_alias_tail(tokens: &mut Vec<TokenWithSpan>, start: usize) {
    // Expand once, moving the remaining tokens forward in linear time.
    // Repeated Vec::insert would make wide SELECT lists quadratic.
    let tail = tokens.split_off(start);
    for mut token in tail {
        let Some(end) = decimal_alias_split(&token.token) else {
            tokens.push(token);
            continue;
        };
        let Token::Word(word) = &mut token.token else {
            unreachable!()
        };
        let alias = word.value.split_off(end);
        let number = std::mem::take(&mut word.value);
        let middle = sqlparser::tokenizer::Location::new(
            token.span.start.line,
            token.span.start.column + end as u64,
        );
        tokens.push(TokenWithSpan::new(
            Token::Number(number, false),
            sqlparser::tokenizer::Span::new(token.span.start, middle),
        ));
        tokens.push(TokenWithSpan::new(
            Token::make_word(&alias, None),
            sqlparser::tokenizer::Span::new(middle, token.span.end),
        ));
    }
}

fn tokenize_hex_prefixes(
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
            && bytes[1] == b'b'
            && bytes[2..].iter().all(|byte| matches!(byte, b'0' | b'1'))
        {
            word.value.drain(..2);
            token.token = Token::SingleQuotedByteStringLiteral(std::mem::take(&mut word.value));
        }
    }
}
