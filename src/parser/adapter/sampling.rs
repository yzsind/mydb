//! Keep REPEATABLE expressions in the parse-local registry: upstream's seed
//! AST accepts only a literal value, while SQL sampling permits expressions.
use super::super::*;
use sqlparser::tokenizer::TokenWithSpan;

fn word(token: &Token, name: &str) -> bool {
    matches!(token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(name))
}

fn close(tokens: &[TokenWithSpan], start: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (i, token) in tokens.iter().enumerate().skip(start) {
        match token.token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

pub(super) fn lower(
    dialect: &dyn Dialect,
    sql: &str,
    tokens: &mut Vec<TokenWithSpan>,
) -> Result<(), ParserError> {
    if !tokens.iter().any(|t| word(&t.token, "TABLESAMPLE")) {
        return Ok(());
    }
    let mut index = 0;
    while index < tokens.len() {
        if !word(&tokens[index].token, "TABLESAMPLE") {
            index += 1;
            continue;
        }
        let next = |from: usize| {
            (from..tokens.len()).find(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
        };
        let Some(mut amount) = next(index + 1) else {
            break;
        };
        if ["BERNOULLI", "SYSTEM", "ROW", "BLOCK"]
            .iter()
            .any(|name| word(&tokens[amount].token, name))
        {
            let Some(pos) = next(amount + 1) else { break };
            amount = pos;
        }
        if tokens[amount].token != Token::LParen {
            index += 1;
            continue;
        }
        let Some(end_amount) = close(tokens, amount) else {
            break;
        };
        let Some(repeatable) = next(end_amount + 1) else {
            break;
        };
        if !word(&tokens[repeatable].token, "REPEATABLE") {
            index += 1;
            continue;
        }
        let Some(open) = next(repeatable + 1) else {
            break;
        };
        if tokens[open].token != Token::LParen {
            index += 1;
            continue;
        }
        let Some(end) = close(tokens, open) else {
            break;
        };
        let mut parser = SqlParser::new(dialect)
            .with_recursion_limit(super::operators::UPSTREAM_RECURSION_LIMIT)
            .with_tokens_with_locations(tokens[open + 1..end].to_vec());
        let expr = parser.parse_expr()?;
        if parser.peek_token_ref().token != Token::EOF {
            return Err(ParserError::ParserError(
                "unexpected tokens in REPEATABLE expression".into(),
            ));
        }
        let name = extensions::register(sql, extensions::Extension::SampleSeed(Box::new(expr)));
        let span = tokens[open].span;
        tokens.splice(
            open + 1..end,
            [TokenWithSpan::new(Token::Placeholder(name), span)],
        );
        index = open + 3;
    }
    Ok(())
}
