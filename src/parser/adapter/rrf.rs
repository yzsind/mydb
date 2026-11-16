//! Token-level RRF query lowering with parse-local extension provenance.
//! Branches retain upstream ASTs and lexical parameter identities; no SQL
//! text is generated or reparsed and user identifiers cannot forge nodes.
use super::super::*;
use sqlparser::tokenizer::{TokenWithSpan, Word};

#[derive(Clone, Debug)]
pub(in crate::parser) struct ParsedRrf {
    pub branches: Vec<(String, Query)>,
    pub rank_constant: Option<SqlExpr>,
    pub weights: Option<Vec<SqlExpr>>,
    pub score_details: bool,
}

fn word(token: &Token, value: &str) -> bool {
    matches!(token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(value))
}
fn fail(message: &str) -> ParserError {
    ParserError::ParserError(format!("RRF: {message}"))
}
fn significant(tokens: &[TokenWithSpan]) -> Vec<usize> {
    tokens
        .iter()
        .enumerate()
        .filter_map(|(i, t)| (!matches!(t.token, Token::Whitespace(_))).then_some(i))
        .collect()
}
fn close(tokens: &[TokenWithSpan], start: usize) -> Result<usize, ParserError> {
    let mut depth = 0usize;
    for (i, token) in tokens.iter().enumerate().skip(start) {
        match token.token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| fail("unbalanced branch"))?;
                if depth == 0 {
                    return Ok(i);
                }
            }
            _ => {}
        }
    }
    Err(fail("unclosed branch"))
}
fn open(tokens: &[TokenWithSpan], end: usize) -> Result<usize, ParserError> {
    let mut depth = 0usize;
    for i in (0..=end).rev() {
        match tokens[i].token {
            Token::RParen => depth += 1,
            Token::LParen => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| fail("unbalanced branch"))?;
                if depth == 0 {
                    return Ok(i);
                }
            }
            _ => {}
        }
    }
    Err(fail("branch must be parenthesized"))
}
fn query(
    dialect: &dyn Dialect,
    sql: &str,
    mut tokens: Vec<TokenWithSpan>,
) -> Result<Query, ParserError> {
    lower(dialect, sql, &mut tokens)?;
    let mut parser = SqlParser::new(dialect)
        .with_recursion_limit(super::operators::UPSTREAM_RECURSION_LIMIT)
        .with_tokens_with_locations(tokens);
    let query = parser.parse_query()?;
    if parser.peek_token_ref().token != Token::EOF {
        return Err(fail("unexpected tokens in branch"));
    }
    Ok(*query)
}
fn alias(
    tokens: &[TokenWithSpan],
    sig: &[usize],
    pos: &mut usize,
    default: usize,
) -> Result<String, ParserError> {
    if sig.get(*pos).is_some_and(|i| word(&tokens[*i].token, "AS")) {
        *pos += 1;
        let Some(Token::Word(name)) = sig.get(*pos).map(|i| &tokens[*i].token) else {
            return Err(fail("expected branch name after AS"));
        };
        *pos += 1;
        Ok(name.value.clone())
    } else {
        Ok(format!("branch_{default}"))
    }
}

pub(in crate::parser) fn lower(
    dialect: &dyn Dialect,
    sql: &str,
    tokens: &mut Vec<TokenWithSpan>,
) -> Result<(), ParserError> {
    if !tokens.iter().any(|token| word(&token.token, "RRF")) {
        return Ok(());
    }
    loop {
        let sig = significant(tokens);
        let candidate = sig.iter().enumerate().find_map(|(p, i)| {
            if !word(&tokens[*i].token, "RRF") || p == 0 {
                return None;
            }
            let right = *sig.get(p + 1)?;
            if tokens[right].token != Token::LParen {
                return None;
            }
            let head = &tokens[*sig.get(p + 2)?].token;
            if !word(head, "SELECT") && !word(head, "WITH") && *head != Token::LParen {
                return None;
            }
            let end = if tokens[sig[p - 1]].token == Token::RParen {
                sig[p - 1]
            } else if p >= 3
                && matches!(tokens[sig[p - 1]].token, Token::Word(_))
                && word(&tokens[sig[p - 2]].token, "AS")
                && tokens[sig[p - 3]].token == Token::RParen
            {
                sig[p - 3]
            } else {
                return None;
            };
            Some((p, end))
        });
        let Some((operator, left_end)) = candidate else {
            return Ok(());
        };
        let start = open(tokens, left_end)?;
        let mut pos = sig.iter().position(|i| *i == left_end).unwrap() + 1;
        let first_name = alias(tokens, &sig, &mut pos, 1)?;
        if pos != operator {
            return Err(fail("unexpected branch alias"));
        }
        let mut parsed = ParsedRrf {
            branches: vec![(
                first_name,
                query(dialect, sql, tokens[start + 1..left_end].to_vec())?,
            )],
            rank_constant: None,
            weights: None,
            score_details: false,
        };
        let mut end = left_end + 1;
        while sig.get(pos).is_some_and(|i| word(&tokens[*i].token, "RRF")) {
            pos += 1;
            let left = *sig
                .get(pos)
                .ok_or_else(|| fail("expected parenthesized branch"))?;
            if tokens[left].token != Token::LParen {
                return Err(fail("expected parenthesized branch"));
            }
            let right = close(tokens, left)?;
            pos = sig.iter().position(|i| *i == right).unwrap() + 1;
            let name = alias(tokens, &sig, &mut pos, parsed.branches.len() + 1)?;
            let branch = query(dialect, sql, tokens[left + 1..right].to_vec())?;
            parsed.branches.push((name, branch));
            end = sig.get(pos).copied().unwrap_or(tokens.len());
            if parsed.branches.len() > 8 {
                return Err(fail("at most 8 branches are supported"));
            }
        }
        if sig
            .get(pos)
            .is_some_and(|i| word(&tokens[*i].token, "WITH"))
        {
            pos += 1;
            let left = *sig.get(pos).ok_or_else(|| fail("expected options"))?;
            if tokens[left].token != Token::LParen {
                return Err(fail("expected parenthesized options"));
            }
            let right = close(tokens, left)?;
            let mut p = SqlParser::new(dialect)
                .with_tokens_with_locations(tokens[left + 1..right].to_vec());
            let mut seen = std::collections::HashSet::new();
            loop {
                let name = p.parse_identifier()?.value.to_ascii_uppercase();
                if !seen.insert(name.clone()) {
                    return Err(fail("duplicate option"));
                }
                p.expect_token(&Token::Eq)?;
                match name.as_str() {
                    "RANK_CONSTANT" => parsed.rank_constant = Some(p.parse_expr()?),
                    "WEIGHTS" => {
                        p.expect_token(&Token::LParen)?;
                        let mut weights = vec![p.parse_expr()?];
                        while p.consume_token(&Token::Comma) {
                            weights.push(p.parse_expr()?);
                        }
                        p.expect_token(&Token::RParen)?;
                        parsed.weights = Some(weights);
                    }
                    "SCORE_DETAILS" => {
                        let token = p.next_token();
                        if word(&token.token, "TRUE") {
                            parsed.score_details = true;
                        } else if word(&token.token, "FALSE") {
                            parsed.score_details = false;
                        } else {
                            return Err(fail("SCORE_DETAILS requires TRUE or FALSE"));
                        }
                    }
                    _ => return Err(fail("unknown option")),
                }
                if !p.consume_token(&Token::Comma) {
                    break;
                }
            }
            if p.peek_token_ref().token != Token::EOF {
                return Err(fail("unexpected option tokens"));
            }
            end = right + 1;
        }
        let name =
            extensions::register(sql, extensions::Extension::Rrf(std::sync::Arc::new(parsed)));
        let span = tokens[start].span;
        let table = TokenWithSpan::new(
            Token::Word(Word {
                value: "SELECT".into(),
                quote_style: None,
                keyword: Keyword::SELECT,
            }),
            span,
        );
        let ident = TokenWithSpan::new(
            Token::Word(Word {
                value: name,
                quote_style: None,
                keyword: Keyword::NoKeyword,
            }),
            span,
        );
        tokens.splice(start..end, [table, ident]);
    }
}
