use super::super::{
    current_parser_sql_mode_bits, parse_lex_error, AccountIdent, ParseError, Span, MYSQL_VERSION_ID,
};
use crate::sql_lex::{SqlLexMode, SqlToken, SqlTokenCursor, SqlTokenKind};

/// Grammar operations preserve token kind, quote identity and byte positions.
pub(super) struct Cursor<'a> {
    sql: &'a str,
    tokens: SqlTokenCursor<'a>,
    position: usize,
}

impl<'a> Cursor<'a> {
    pub(super) fn new(sql: &'a str) -> Self {
        Self {
            sql,
            position: 0,
            tokens: SqlTokenCursor::new(
                sql,
                SqlLexMode::from_parser_bits(current_parser_sql_mode_bits()),
                MYSQL_VERSION_ID,
            ),
        }
    }

    pub(super) fn next(&mut self) -> Result<Option<SqlToken>, ParseError> {
        let token = self
            .tokens
            .next()
            .map_err(|err| parse_lex_error(self.sql, err))?;
        if let Some(token) = &token {
            self.position = token.span.end;
        }
        Ok(token)
    }

    pub(super) fn peek(&mut self) -> Result<Option<SqlToken>, ParseError> {
        self.tokens
            .peek()
            .map(|token| token.cloned())
            .map_err(|err| parse_lex_error(self.sql, err))
    }

    pub(super) fn keyword(&mut self, word: &[u8]) -> Result<bool, ParseError> {
        let token = self
            .tokens
            .eat_keyword_token(word)
            .map_err(|err| parse_lex_error(self.sql, err))?;
        if let Some(token) = &token {
            self.position = token.span.end;
        }
        Ok(token.is_some())
    }

    pub(super) fn expect_keyword(&mut self, word: &[u8]) -> Result<(), ParseError> {
        if self.keyword(word)? {
            Ok(())
        } else {
            Err(self.expected(std::str::from_utf8(word).unwrap()))
        }
    }

    pub(super) fn symbol(&mut self, symbol: u8) -> Result<bool, ParseError> {
        if self
            .peek()?
            .is_some_and(|token| token.kind == SqlTokenKind::Symbol(symbol))
        {
            self.next()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(super) fn source(&self) -> &'a str {
        self.sql
    }
    pub(super) fn quoted_text(&self, token: &SqlToken) -> Result<String, ParseError> {
        self.tokens.decoded_quoted_text(token).ok_or_else(|| {
            ParseError::new(
                self.sql,
                Span {
                    start: token.span.start,
                    end: token.span.end,
                },
                "expected quoted name",
                vec!["quoted name".into()],
            )
        })
    }
    pub(super) fn position(&self) -> usize {
        self.position
    }
    pub(super) fn text(&self, token: &SqlToken) -> &'a str {
        self.tokens.token_text(token)
    }
    pub(super) fn expect_symbol(&mut self, symbol: u8) -> Result<(), ParseError> {
        if self.symbol(symbol)? {
            Ok(())
        } else {
            Err(self.expected(&(symbol as char).to_string()))
        }
    }

    pub(super) fn expected(&mut self, expected: &str) -> ParseError {
        let token = self.peek().ok().flatten();
        let span = token.map_or(
            Span {
                start: self.sql.len(),
                end: self.sql.len(),
            },
            |token| Span {
                start: token.span.start,
                end: token.span.end,
            },
        );
        ParseError::new(
            self.sql,
            span,
            format!("expected {expected}"),
            vec![expected.into()],
        )
    }

    fn name(&mut self, strings: bool) -> Result<String, ParseError> {
        let token = self.peek()?.ok_or_else(|| self.expected("identifier"))?;
        let name = match token.kind {
            SqlTokenKind::Word => self.tokens.token_text(&token).to_owned(),
            SqlTokenKind::QuotedIdentifier => self.tokens.decoded_quoted_text(&token).unwrap(),
            SqlTokenKind::String if strings => self.tokens.decoded_quoted_text(&token).unwrap(),
            _ => return Err(self.expected("identifier")),
        };
        self.next()?;
        if name.is_empty() && !strings {
            return Err(self.expected("nonempty identifier"));
        }
        Ok(name)
    }

    pub(super) fn account_text(&mut self) -> Result<String, ParseError> {
        if let Some(token) = self.peek()? {
            if token.kind == SqlTokenKind::Number {
                self.next()?;
                return Ok(self.text(&token).to_owned());
            }
        }
        self.name(true)
    }

    pub(super) fn identifier(&mut self) -> Result<String, ParseError> {
        self.name(false)
    }

    pub(super) fn qualified_name(&mut self) -> Result<super::super::QualifiedName, ParseError> {
        let start = self
            .peek()?
            .map_or(self.sql.len(), |token| token.span.start);
        let first = self.identifier()?;
        let (database, object) = if self.symbol(b'.')? {
            (Some(first), self.identifier()?)
        } else {
            (None, first)
        };
        super::super::QualifiedName::new(database, object).map_err(|message| {
            ParseError::new(
                self.sql,
                Span {
                    start,
                    end: self.position,
                },
                message,
                Vec::new(),
            )
        })
    }

    pub(super) fn account(&mut self) -> Result<AccountIdent, ParseError> {
        let user = self.name(true)?;
        let host = if self.symbol(b'@')? {
            self.name(true)?
        } else {
            "%".to_owned()
        };
        Ok(AccountIdent { user, host })
    }

    pub(super) fn accounts(&mut self) -> Result<Vec<AccountIdent>, ParseError> {
        let mut accounts = vec![self.account()?];
        while self.symbol(b',')? {
            accounts.push(self.account()?);
        }
        Ok(accounts)
    }

    pub(super) fn finish(&mut self) -> Result<(), ParseError> {
        let _ = self.symbol(b';')?;
        if self.peek()?.is_some() {
            Err(self.expected("end of statement"))
        } else {
            Ok(())
        }
    }
}
