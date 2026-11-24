//! Lower MySQL parenthesized comma table lists without touching expression
//! commas, quoted text, or source coordinates. Each comma item is scoped
//! separately so explicit joins retain their higher precedence.
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Token, TokenWithSpan, Word};

pub(super) fn lower(tokens: &mut Vec<TokenWithSpan>) {
    if !tokens.iter().any(|t| t.token == Token::Comma) {
        return;
    }
    let significant: Vec<_> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t.token, Token::Whitespace(_)))
        .map(|(i, _)| i)
        .collect();
    let mut stack: Vec<(bool, usize, Vec<usize>)> = Vec::new();
    let mut groups = Vec::new();
    let mut from_scopes = vec![false];
    for (position, &index) in significant.iter().enumerate() {
        match &tokens[index].token {
            Token::Word(word)
                if word.quote_style.is_none()
                    && matches!(
                        word.keyword,
                        Keyword::SELECT | Keyword::WITH | Keyword::VALUES
                    ) =>
            {
                if let Some(group) = stack.last_mut() {
                    group.0 = false;
                }
                *from_scopes.last_mut().unwrap() = false;
            }
            Token::Word(word)
                if word.quote_style.is_none()
                    && matches!(word.keyword, Keyword::FROM | Keyword::JOIN) =>
            {
                *from_scopes.last_mut().unwrap() = true;
            }
            Token::Word(word)
                if word.quote_style.is_none()
                    && matches!(
                        word.keyword,
                        Keyword::WHERE
                            | Keyword::GROUP
                            | Keyword::HAVING
                            | Keyword::ORDER
                            | Keyword::LIMIT
                            | Keyword::UNION
                            | Keyword::EXCEPT
                            | Keyword::INTERSECT
                    ) =>
            {
                *from_scopes.last_mut().unwrap() = false;
            }
            Token::LParen => {
                let previous = position
                    .checked_sub(1)
                    .map(|p| &tokens[significant[p]].token);
                let next_is_query = significant.get(position + 1).is_some_and(|&i| matches!(&tokens[i].token, Token::Word(w) if matches!(w.keyword, Keyword::SELECT | Keyword::WITH | Keyword::VALUES)));
                let table = !next_is_query
                    && (matches!(previous, Some(Token::Word(w)) if matches!(w.keyword, Keyword::FROM | Keyword::JOIN | Keyword::STRAIGHT_JOIN))
                        || (stack.last().is_some_and(|g| g.0)
                            && matches!(previous, Some(Token::LParen | Token::Comma)))
                        || (*from_scopes.last().unwrap()
                            && matches!(previous, Some(Token::Comma))));
                stack.push((table, index, Vec::new()));
                from_scopes.push(false);
            }
            Token::Comma => {
                if let Some((true, _, commas)) = stack.last_mut() {
                    commas.push(index);
                }
            }
            Token::RParen => {
                if from_scopes.len() > 1 {
                    from_scopes.pop();
                }
                if let Some((true, start, commas)) = stack.pop() {
                    if !commas.is_empty() {
                        groups.push((start, index, commas));
                    }
                }
            }
            _ => {}
        }
    }
    if groups.is_empty() {
        return;
    }
    let mut before = std::collections::BTreeMap::<usize, Vec<Token>>::new();
    let mut replace = std::collections::BTreeMap::new();
    for (start, end, commas) in groups {
        before.entry(start + 1).or_default().push(Token::LParen);
        before.entry(end).or_default().push(Token::RParen);
        for comma in commas {
            replace.insert(
                comma,
                vec![
                    Token::RParen,
                    Token::Word(Word {
                        value: "CROSS".into(),
                        quote_style: None,
                        keyword: Keyword::CROSS,
                    }),
                    Token::Word(Word {
                        value: "JOIN".into(),
                        quote_style: None,
                        keyword: Keyword::JOIN,
                    }),
                    Token::LParen,
                ],
            );
        }
    }
    let mut out = Vec::with_capacity(tokens.len() + before.len() + replace.len() * 3);
    for (index, token) in tokens.drain(..).enumerate() {
        if let Some(prefix) = before.remove(&index) {
            out.extend(
                prefix
                    .into_iter()
                    .map(|t| TokenWithSpan::new(t, token.span)),
            );
        }
        if let Some(replacement) = replace.remove(&index) {
            out.extend(
                replacement
                    .into_iter()
                    .map(|t| TokenWithSpan::new(t, token.span)),
            );
        } else {
            out.push(token);
        }
    }
    *tokens = out;
}
