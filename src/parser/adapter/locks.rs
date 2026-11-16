//! sqlparser models one OF target per clause. Expand a target list into
//! equivalent typed token clauses without rewriting text or source locations.
use sqlparser::{
    keywords::Keyword,
    tokenizer::{Token, TokenWithSpan},
};

pub(super) fn expand_lists(tokens: &mut Vec<TokenWithSpan>) {
    if !tokens.iter().any(|token| matches!(&token.token, Token::Word(word) if word.quote_style.is_none() && word.keyword == Keyword::OF)) {
        return;
    }
    let indices: Vec<_> = tokens
        .iter()
        .enumerate()
        .filter_map(|(index, token)| {
            (!matches!(token.token, Token::Whitespace(_))).then_some(index)
        })
        .collect();
    let keyword = |at: usize, expected: Keyword| {
        indices.get(at).is_some_and(|&index| matches!(&tokens[index].token, Token::Word(word) if word.quote_style.is_none() && word.keyword == expected))
    };
    let symbol = |at: usize, expected: &Token| {
        indices
            .get(at)
            .is_some_and(|&index| tokens[index].token == *expected)
    };
    let mut edits = Vec::new();
    let mut at = 0;
    while at + 3 < indices.len() {
        if !keyword(at, Keyword::FOR)
            || !(keyword(at + 1, Keyword::UPDATE) || keyword(at + 1, Keyword::SHARE))
            || !keyword(at + 2, Keyword::OF)
        {
            at += 1;
            continue;
        }
        let begin = at;
        let mut names = Vec::new();
        at += 3;
        loop {
            let name_start = at;
            if !indices
                .get(at)
                .is_some_and(|&index| matches!(tokens[index].token, Token::Word(_)))
            {
                break;
            }
            at += 1;
            while symbol(at, &Token::Period)
                && indices
                    .get(at + 1)
                    .is_some_and(|&index| matches!(tokens[index].token, Token::Word(_)))
            {
                at += 2;
            }
            names.push(name_start..at);
            if !symbol(at, &Token::Comma) {
                break;
            }
            at += 1;
        }
        let wait_start = at;
        if keyword(at, Keyword::NOWAIT) {
            at += 1;
        } else if keyword(at, Keyword::SKIP) && keyword(at + 1, Keyword::LOCKED) {
            at += 2;
        }
        if names.len() < 2 {
            continue;
        }
        let mut replacement = Vec::new();
        for name in names {
            replacement.extend((begin..begin + 3).map(|index| tokens[indices[index]].clone()));
            replacement.extend(name.map(|index| tokens[indices[index]].clone()));
            replacement.extend((wait_start..at).map(|index| tokens[indices[index]].clone()));
        }
        edits.push((indices[begin]..indices[at - 1] + 1, replacement));
    }
    if edits.is_empty() {
        return;
    }
    let mut source = std::mem::take(tokens).into_iter();
    let mut consumed = 0;
    for (range, replacement) in edits {
        tokens.extend(source.by_ref().take(range.start - consumed));
        source.by_ref().take(range.len()).for_each(drop);
        tokens.extend(replacement);
        consumed = range.end;
    }
    tokens.extend(source);
}
