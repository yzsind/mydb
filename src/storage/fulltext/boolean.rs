//! IN BOOLEAN MODE query parser.
//!
//! MySQL boolean full-text search is a flat list of operator-prefixed terms
//! (no deep nesting in the subset we support). Each term carries an
//! "occurrence" requirement and a matcher:
//!
//! - `+word`   → [`Occur::Must`]    (the row must contain it)
//! - `-word`   → [`Occur::MustNot`] (the row must not contain it)
//! - `word`    → [`Occur::Should`]  (optional; contributes to relevance, and
//!                                    when no `+` term exists, defines the
//!                                    candidate set as the union of Shoulds)
//! - `"a b c"` → phrase: the tokens must appear adjacent and in order
//! - `word*`   → prefix wildcard: any token starting with `word`
//!
//! Operands are tokenised with the index's own [`Tokenizer`] so build-side
//! and query-side tokens always agree (a CJK ngram word becomes several
//! n-gram tokens, matched as a conjunction / phrase). `( )` grouping,
//! relevance-weight operators `> < ~`, `@` proximity and `WITH QUERY
//! EXPANSION` are rejected explicitly (operators: 1235; query expansion:
//! 1064 in the SQL parser)
//! rather than silently discarded.

use super::Tokenizer;

/// How a clause must occur in a matching document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Occur {
    Must,
    MustNot,
    Should,
}

/// What a clause matches against the inverted index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Matcher {
    /// One operand word, tokenised. Multiple tokens (e.g. a CJK ngram word)
    /// must all be present (conjunction).
    Term(Vec<String>),
    /// `word*` — any indexed token whose text starts with this (lower-cased)
    /// prefix.
    Prefix(String),
    /// `"a b c"` — the tokens must appear contiguous and in order.
    Phrase(Vec<String>),
}

/// One parsed boolean-mode clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoolClause {
    pub occur: Occur,
    pub matcher: Matcher,
}

/// Parse a boolean-mode query string into clauses, tokenising each operand
/// with `tokenizer`. Empty operands (all stopwords / too short) are dropped.
///
/// **Unsupported operators error out (never silently)**: parentheses `( )`
/// for grouping and the InnoDB relevance-weight operators `> < ~` return
/// error 1235 with a clear message. Silently discarding them (the previous
/// behavior) would make e.g. `+(a b) -c` behave as `+a b -c` and mislead
/// users into thinking grouping worked. The `@dist` proximity operator is
/// also rejected. Callers propagate the error to the client.
pub fn parse_boolean_query(
    query: &str,
    tokenizer: &dyn Tokenizer,
) -> Result<Vec<BoolClause>, crate::types::QueryError> {
    let unsupported_op = |op: char| -> crate::types::QueryError {
        crate::error::catalog::ER_NOT_SUPPORTED_YET.error(format!(
            "MATCH ... AGAINST(... IN BOOLEAN MODE) operator '{op}' is not supported"
        ))
    };
    let mut clauses = Vec::new();
    let malformed = || {
        crate::error::catalog::ER_PARSE_ERROR
            .error("Invalid operand in MATCH ... AGAINST(... IN BOOLEAN MODE)")
    };
    let mut chars = query.chars().peekable();
    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        // Operators outside quoted phrases must never become plain tokens.
        if matches!(ch, '(' | ')' | '>' | '<' | '~' | '@') {
            return Err(unsupported_op(ch));
        }
        let occur = match ch {
            '+' => {
                chars.next();
                Occur::Must
            }
            '-' => {
                chars.next();
                Occur::MustNot
            }
            _ => Occur::Should,
        };
        if occur != Occur::Should
            && chars
                .peek()
                .is_none_or(|c| c.is_whitespace() || matches!(c, '+' | '-'))
        {
            return Err(malformed());
        }
        if matches!(chars.peek(), Some('"')) {
            chars.next(); // opening quote
            let mut phrase = String::new();
            let mut closed = false;
            for c in chars.by_ref() {
                if c == '"' {
                    closed = true;
                    break;
                }
                phrase.push(c);
            }
            if !closed {
                return Err(malformed());
            }
            let tokens = tokenizer.tokenize(&phrase);
            if !tokens.is_empty() {
                clauses.push(BoolClause {
                    occur,
                    matcher: Matcher::Phrase(tokens),
                });
            }
            continue;
        }
        // Plain word: read until whitespace or a quote.
        let mut word = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_whitespace() || c == '"' {
                break;
            }
            if matches!(c, '(' | ')' | '>' | '<' | '~' | '@') {
                return Err(unsupported_op(c));
            }
            word.push(c);
            chars.next();
            if c == '*' {
                if word.len() == 1
                    || chars
                        .peek()
                        .is_some_and(|next| !next.is_whitespace() && *next != '"')
                {
                    return Err(unsupported_op('*'));
                }
                break;
            }
        }
        if word.ends_with(['+', '-']) {
            return Err(malformed());
        }
        if let Some(stripped) = word.strip_suffix('*') {
            let prefix = stripped.to_lowercase();
            if !prefix.is_empty() {
                clauses.push(BoolClause {
                    occur,
                    matcher: Matcher::Prefix(prefix),
                });
            }
            continue;
        }
        let tokens = tokenizer.tokenize(&word);
        if !tokens.is_empty() {
            clauses.push(BoolClause {
                occur,
                matcher: if tokenizer.parser_name() == "ngram" && tokens.len() > 1 {
                    Matcher::Phrase(tokens)
                } else {
                    Matcher::Term(tokens)
                },
            });
        }
    }
    Ok(clauses)
}

/// Exclusive upper bound among UTF-8 string keys. Unicode scalar order agrees
/// with UTF-8 byte order; skip surrogates and carry past maximal scalars.
pub fn prefix_successor(prefix: &str) -> Option<String> {
    for (offset, ch) in prefix.char_indices().rev() {
        let next = match ch as u32 {
            0x10ffff => continue,
            0xd7ff => 0xe000,
            value => value + 1,
        };
        let mut successor = prefix[..offset].to_owned();
        successor.push(char::from_u32(next).expect("valid scalar successor"));
        return Some(successor);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::fulltext::build_tokenizer;

    fn default_tok() -> Box<dyn Tokenizer> {
        build_tokenizer("", None)
    }

    #[test]
    fn parses_must_and_must_not() {
        let c = parse_boolean_query("+database -mysql", default_tok().as_ref()).unwrap();
        assert_eq!(
            c,
            vec![
                BoolClause {
                    occur: Occur::Must,
                    matcher: Matcher::Term(vec!["database".into()])
                },
                BoolClause {
                    occur: Occur::MustNot,
                    matcher: Matcher::Term(vec!["mysql".into()])
                },
            ]
        );
    }

    #[test]
    fn parses_phrase() {
        let c = parse_boolean_query("\"quick brown fox\"", default_tok().as_ref()).unwrap();
        assert_eq!(
            c,
            vec![BoolClause {
                occur: Occur::Should,
                matcher: Matcher::Phrase(vec!["quick".into(), "brown".into(), "fox".into()]),
            }]
        );
    }

    #[test]
    fn parses_must_phrase() {
        let c = parse_boolean_query("+\"data structures\"", default_tok().as_ref()).unwrap();
        assert_eq!(
            c,
            vec![BoolClause {
                occur: Occur::Must,
                matcher: Matcher::Phrase(vec!["data".into(), "structures".into()]),
            }]
        );
    }

    #[test]
    fn audit_invalid_truncation() {
        for query in ["*mysql", "+*mysql", "data*base*", "mysql**", "*"] {
            assert_eq!(
                parse_boolean_query(query, default_tok().as_ref())
                    .unwrap_err()
                    .code(),
                1235
            );
        }
    }

    #[test]
    fn parses_prefix() {
        let c = parse_boolean_query("data*", default_tok().as_ref()).unwrap();
        assert_eq!(
            c,
            vec![BoolClause {
                occur: Occur::Should,
                matcher: Matcher::Prefix("data".into()),
            }]
        );
        // `+` prefix combines occur + wildcard.
        let c = parse_boolean_query("+data*", default_tok().as_ref()).unwrap();
        assert_eq!(c[0].occur, Occur::Must);
        assert_eq!(c[0].matcher, Matcher::Prefix("data".into()));
    }

    #[test]
    fn drops_stopword_and_short_operands() {
        // "a" is a stopword / too short → no clause.
        assert!(parse_boolean_query("a", default_tok().as_ref())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn ngram_word_becomes_phrase() {
        let tok = build_tokenizer("ngram", Some(2));
        let c = parse_boolean_query("+数据库", tok.as_ref()).unwrap();
        assert_eq!(
            c,
            vec![BoolClause {
                occur: Occur::Must,
                matcher: Matcher::Phrase(vec!["数据".into(), "据库".into()]),
            }]
        );
    }

    #[test]
    fn unsupported_operators_error_1235() {
        // Silent discard was the previous bug: e.g. `(a b)` would parse as
        // `a b`, misleading users into thinking grouping worked.
        for q in &["(database)", ">weighty", "<light", "~exclude", "@10"] {
            let err = parse_boolean_query(q, default_tok().as_ref())
                .expect_err(&format!("expected 1235 for {q}"));
            assert_eq!(err.code(), 1235, "query {q}: {}", err.message());
            assert!(
                err.message().contains("is not supported"),
                "query {q}: {}",
                err.message()
            );
        }
    }

    #[test]
    fn operator_char_in_word_still_parses() {
        // Only top-level operator characters are rejected; embedded ones fall
        // through to the tokenizer's normal splitting.
        let c = parse_boolean_query("foo-bar", default_tok().as_ref()).unwrap();
        assert!(!c.is_empty(), "foo-bar should tokenise into foo/bar");
    }

    #[test]
    fn fulltext_unicode_prefix_successor() {
        for (prefix, expected) in [
            ("丿", "乀"),
            ("\u{7f}", "\u{80}"),
            ("\u{7ff}", "\u{800}"),
            ("\u{d7ff}", "\u{e000}"),
            ("\u{ffff}", "\u{10000}"),
            ("a\u{10ffff}", "b"),
        ] {
            assert_eq!(prefix_successor(prefix).as_deref(), Some(expected));
        }
        assert_eq!(prefix_successor("\u{10ffff}"), None);
        assert_eq!(prefix_successor(""), None);
    }

    #[test]
    fn prefix_successor_increments_last_byte() {
        assert_eq!(prefix_successor("data").as_deref(), Some("datb"));
        // Only the last byte is bumped: 'z' (0x7A) → '{' (0x7B).
        assert_eq!(prefix_successor("az").as_deref(), Some("a{"));
    }
}
