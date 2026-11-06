//! Row security grammar. Expressions use the canonical scalar parser.
use super::{Expr, ParseError, Statement};
use crate::engine::auth::AccountKey;
use serde::{Deserialize, Serialize};
use sqlparser::tokenizer::{Token, Tokenizer};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Command {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyColumnReference {
    pub node: u32,
    pub column_id: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Policy {
    pub name: String,
    pub permissive: bool,
    pub command: Command,
    pub public: bool,
    pub roles: Vec<AccountKey>,
    pub using: Option<Expr>,
    pub check: Option<Expr>,
    #[serde(default)]
    pub table_dependencies: Vec<(u64, String)>,
    #[serde(default)]
    pub using_columns: Vec<PolicyColumnReference>,
    #[serde(default)]
    pub check_columns: Vec<PolicyColumnReference>,
    #[serde(default)]
    pub using_settings: Option<std::sync::Arc<crate::engine::expr::PolicyExpressionSettings>>,
    #[serde(default)]
    pub check_settings: Option<std::sync::Arc<crate::engine::expr::PolicyExpressionSettings>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RlsStatement {
    Create {
        table: String,
        policy: Policy,
    },
    Alter {
        table: String,
        name: String,
        roles: Option<(bool, Vec<AccountKey>)>,
        using: Option<Expr>,
        check: Option<Expr>,
        rename: Option<String>,
    },
    Drop {
        table: String,
        name: String,
        if_exists: bool,
    },
    Enable {
        table: String,
        enabled: bool,
    },
    Show {
        table: String,
        name: String,
    },
    SetContext {
        local: bool,
        key: String,
        value: Expr,
    },
    ResetContext {
        key: Option<String>,
    },
}

fn err(message: &str) -> ParseError {
    ParseError::at_zero(message)
}

struct Cursor<'a> {
    tokens: Vec<Token>,
    ranges: Vec<std::ops::Range<usize>>,
    raw: &'a str,
    pos: usize,
}
impl Cursor<'_> {
    fn eat(&mut self, word: &str) -> bool {
        if matches!(self.tokens.get(self.pos), Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
        {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, word: &str) -> Result<(), ParseError> {
        if self.eat(word) {
            Ok(())
        } else {
            Err(err(&format!("Expected {word} in row security statement")))
        }
    }
    fn symbol(&mut self, token: Token) -> bool {
        if self.tokens.get(self.pos) == Some(&token) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn name(&mut self) -> Result<String, ParseError> {
        let value = match self.tokens.get(self.pos) {
            Some(Token::Word(w)) => w.value.clone(),
            Some(Token::SingleQuotedString(s)) => s.clone(),
            _ => return Err(err("Expected row security identifier")),
        };
        if value.is_empty() || value.chars().count() > 64 {
            return Err(err("Invalid row security identifier"));
        }
        self.pos += 1;
        Ok(value)
    }
    fn table(&mut self) -> Result<String, ParseError> {
        let first = self.name()?;
        if self.symbol(Token::Period) {
            Ok(format!("{}.{}", first, self.name()?))
        } else {
            Ok(first)
        }
    }
    fn roles(&mut self) -> Result<(bool, Vec<AccountKey>), ParseError> {
        let mut public = false;
        let mut roles = Vec::new();
        loop {
            if self.eat("PUBLIC") {
                public = true;
            } else {
                let name = self.name()?;
                let host = if self.symbol(Token::AtSign) {
                    self.name()?
                } else {
                    "%".into()
                };
                roles.push(AccountKey::new(name, host));
            }
            if !self.symbol(Token::Comma) {
                break;
            }
        }
        Ok((public, roles))
    }
    fn expression(&mut self) -> Result<Expr, ParseError> {
        if !self.symbol(Token::LParen) {
            return Err(err("Policy expression requires parentheses"));
        }
        let start = self.ranges[self.pos - 1].end;
        let mut depth = 1;
        while let Some(token) = self.tokens.get(self.pos) {
            match token {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                _ => {}
            }
            if depth == 0 {
                let end = self.ranges[self.pos].start;
                self.pos += 1;
                return super::parse_expr_fragment(&self.raw[start..end])
                    .map_err(|e| e.rebase(self.raw, start));
            }
            self.pos += 1;
        }
        Err(err("Unclosed policy expression"))
    }
    fn finish(&self) -> Result<(), ParseError> {
        if self.pos == self.tokens.len() {
            Ok(())
        } else {
            Err(err("Unexpected trailing row security clause"))
        }
    }
}

pub(super) fn parse(sql: &str) -> Option<Result<Statement, ParseError>> {
    // Avoid tokenization on the ordinary SELECT/DML hot path.
    let trimmed = sql.trim_start();
    let head_len = trimmed.bytes().take_while(u8::is_ascii_alphabetic).count();
    let first = &trimmed[..head_len];
    if !["CREATE", "ALTER", "DROP", "SHOW", "SET", "RESET"]
        .iter()
        .any(|s| first.eq_ignore_ascii_case(s))
    {
        return None;
    }
    let located = Tokenizer::new(&super::MydbDialect::default(), sql)
        .tokenize_with_location()
        .ok()?;
    let mut position = super::source::SourcePosition::start();
    let mut tokens = Vec::new();
    let mut ranges = Vec::new();
    for located in located {
        if matches!(located.token, Token::Whitespace(_) | Token::EOF) {
            continue;
        }
        let start = position.seek(sql, located.span.start.line, located.span.start.column)?;
        let end = position.seek(sql, located.span.end.line, located.span.end.column)?;
        ranges.push(start..end);
        tokens.push(located.token);
    }
    if tokens.last() == Some(&Token::SemiColon) {
        tokens.pop();
        ranges.pop();
    }
    let mut c = Cursor {
        tokens,
        ranges,
        raw: sql,
        pos: 0,
    };
    let action = if c.eat("CREATE") {
        "create"
    } else if c.eat("ALTER") {
        "alter"
    } else if c.eat("DROP") {
        "drop"
    } else if c.eat("SHOW") {
        "show"
    } else if c.eat("SET") {
        "set"
    } else if c.eat("RESET") {
        "reset"
    } else {
        return None;
    };
    if action == "set" {
        let local = c.eat("LOCAL");
        if !c.eat("RLS") {
            return None;
        }
        return Some((|| {
            c.expect("CONTEXT")?;
            let key = c.name()?;
            if !c.symbol(Token::Eq) {
                return Err(err("Expected '=' after RLS context key"));
            }
            let start = c
                .ranges
                .get(c.pos)
                .ok_or_else(|| err("Missing RLS context value"))?
                .start;
            let end = c
                .ranges
                .last()
                .ok_or_else(|| err("Missing RLS context value"))?
                .end;
            let value = super::parse_expr_fragment(&c.raw[start..end])
                .map_err(|e| e.rebase(c.raw, start))?;
            if !matches!(value, Expr::Literal(_) | Expr::Placeholder(_)) {
                return Err(err("RLS context requires a literal or parameter"));
            }
            Ok(Statement::RowSecurity(RlsStatement::SetContext {
                local,
                key,
                value,
            }))
        })());
    }
    if action == "reset" {
        if !c.eat("RLS") {
            return None;
        }
        return Some((|| {
            c.expect("CONTEXT")?;
            let key = if c.pos < c.tokens.len() {
                Some(c.name()?)
            } else {
                None
            };
            c.finish()?;
            Ok(Statement::RowSecurity(RlsStatement::ResetContext { key }))
        })());
    }
    if action == "alter" && c.eat("TABLE") {
        let table = c.table().ok()?;
        let enabled = if c.eat("ENABLE") {
            true
        } else if c.eat("DISABLE") {
            false
        } else {
            if c.eat("FORCE") || (c.eat("NO") && c.eat("FORCE")) {
                if c.eat("ROW") && c.eat("LEVEL") && c.eat("SECURITY") {
                    return Some(Err(err(
                        "FORCE ROW LEVEL SECURITY is inapplicable: table owners are not exempt",
                    )));
                }
            }
            return None;
        };
        // ENABLE/DISABLE KEYS and FORCE rebuild are existing MySQL actions.
        // Claim only the ROW LEVEL SECURITY grammar island.
        if !c.eat("ROW") {
            return None;
        }
        return Some((|| {
            c.expect("LEVEL")?;
            c.expect("SECURITY")?;
            c.finish()?;
            Ok(Statement::RowSecurity(RlsStatement::Enable {
                table,
                enabled,
            }))
        })());
    }
    if action == "show" && !c.eat("CREATE") {
        return None;
    }
    if !c.eat("POLICY") {
        return None;
    }
    Some((|| {
        let if_exists = action == "drop" && c.eat("IF");
        if if_exists {
            c.expect("EXISTS")?;
        }
        let name = c.name()?;
        c.expect("ON")?;
        let table = c.table()?;
        let command = match action {
            "drop" => RlsStatement::Drop {
                table,
                name,
                if_exists,
            },
            "show" => RlsStatement::Show { table, name },
            "create" => {
                let permissive = if c.eat("AS") {
                    if c.eat("PERMISSIVE") {
                        true
                    } else {
                        c.expect("RESTRICTIVE")?;
                        false
                    }
                } else {
                    true
                };
                let command = if c.eat("FOR") {
                    if c.eat("ALL") {
                        Command::All
                    } else if c.eat("SELECT") {
                        Command::Select
                    } else if c.eat("INSERT") {
                        Command::Insert
                    } else if c.eat("UPDATE") {
                        Command::Update
                    } else if c.eat("DELETE") {
                        Command::Delete
                    } else {
                        return Err(err("Invalid policy command"));
                    }
                } else {
                    Command::All
                };
                let (public, roles) = if c.eat("TO") {
                    c.roles()?
                } else {
                    (true, Vec::new())
                };
                let using = if c.eat("USING") {
                    Some(c.expression()?)
                } else {
                    None
                };
                let check = if c.eat("WITH") {
                    c.expect("CHECK")?;
                    Some(c.expression()?)
                } else {
                    None
                };
                if command == Command::Insert && using.is_some()
                    || matches!(command, Command::Select | Command::Delete) && check.is_some()
                {
                    return Err(err("Policy clause is not valid for this command"));
                }
                RlsStatement::Create {
                    table,
                    policy: Policy {
                        name,
                        permissive,
                        command,
                        public,
                        roles,
                        using,
                        check,
                        table_dependencies: Vec::new(),
                        using_columns: Vec::new(),
                        check_columns: Vec::new(),
                        using_settings: None,
                        check_settings: None,
                    },
                }
            }
            "alter" => {
                let rename = if c.eat("RENAME") {
                    c.expect("TO")?;
                    Some(c.name()?)
                } else {
                    None
                };
                let roles = if c.eat("TO") { Some(c.roles()?) } else { None };
                let using = if c.eat("USING") {
                    Some(c.expression()?)
                } else {
                    None
                };
                let check = if c.eat("WITH") {
                    c.expect("CHECK")?;
                    Some(c.expression()?)
                } else {
                    None
                };
                if rename.is_none() && roles.is_none() && using.is_none() && check.is_none() {
                    return Err(err("ALTER POLICY requires a change"));
                }
                if rename.is_some() && (roles.is_some() || using.is_some() || check.is_some()) {
                    return Err(err("RENAME cannot be combined with other policy changes"));
                }
                RlsStatement::Alter {
                    table,
                    name,
                    roles,
                    using,
                    check,
                    rename,
                }
            }
            _ => return Err(err("Invalid policy statement")),
        };
        c.finish()?;
        Ok(Statement::RowSecurity(command))
    })())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_security_does_not_capture_other_alter_actions() {
        for sql in [
            "ALTER TABLE t FORCE",
            "ALTER TABLE t ENABLE KEYS",
            "ALTER TABLE t DISABLE KEYS",
        ] {
            assert!(parse(sql).is_none(), "{sql}");
            assert!(crate::parser::parse_statement(sql).is_ok(), "{sql}");
        }
        for sql in [
            "ALTER TABLE t FORCE ROW LEVEL SECURITY",
            "ALTER TABLE t NO FORCE ROW LEVEL SECURITY",
        ] {
            assert!(parse(sql).unwrap().is_err(), "{sql}");
        }
    }

    #[test]
    fn policy_expressions_keep_original_literal_spelling_and_spacing() {
        for expression in [
            r"payload='a\\b'",
            r"payload=_utf8mb4'a\\b'",
            "payload='é田(x)'",
            "tenant_id=CAST(RLS_CONTEXT('tenant_id') AS UNSIGNED)",
        ] {
            let Statement::RowSecurity(RlsStatement::Create { policy, .. }) = parse(&format!(
                "CREATE/* comment */POLICY p ON t USING({expression})"
            ))
            .unwrap()
            .unwrap() else {
                panic!("expected policy")
            };
            assert_eq!(
                policy.using,
                Some(super::super::parse_expr_fragment(expression).unwrap()),
                "{expression}"
            );
        }
        assert!(
            parse("CREATE POLICY p ON t USING(CAST (tenant_id AS UNSIGNED)=1)")
                .unwrap()
                .is_err()
        );
    }
}
