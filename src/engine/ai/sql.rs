//! SQL grammar and execution boundaries for external effects.
use super::*;
use crate::parser::{Expr, Statement};
use sqlparser::tokenizer::{Token, Tokenizer};

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AiStatement {
    pub action: String,
    pub kind: String,
    pub name: String,
    pub connection: Option<String>,
    pub config: Option<String>,
    #[serde(skip_serializing)]
    pub value: Option<Expr>,
    pub if_exists: bool,
    pub tail: Option<String>,
}
impl std::fmt::Debug for AiStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("AiStatement")
            .field("action", &self.action)
            .field("kind", &self.kind)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}
struct Tokens {
    v: Vec<Token>,
    i: usize,
}
impl Tokens {
    fn eat(&mut self, s: &str) -> bool {
        if self
            .v
            .get(self.i)
            .is_some_and(|t| matches!(t,Token::Word(w)if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(s)))
        {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, s: &str) -> Result<(), crate::parser::ParseError> {
        if self.eat(s) {
            Ok(())
        } else {
            Err(parse_error())
        }
    }
    fn name(&mut self) -> Result<String, crate::parser::ParseError> {
        let mut s = String::new();
        loop {
            let Some(Token::Word(w)) = self.v.get(self.i) else {
                return Err(parse_error());
            };
            if w.value.is_empty()
                || w.value.len() > 64
                || !w.value.bytes().enumerate().all(|(i, b)| {
                    b.is_ascii_alphanumeric() && (i > 0 || !b.is_ascii_digit()) || b == b'_'
                })
            {
                return Err(parse_error());
            }
            if !s.is_empty() {
                s.push('.');
            }
            s.push_str(&w.value.to_ascii_lowercase());
            self.i += 1;
            if self.v.get(self.i) == Some(&Token::Period) {
                self.i += 1;
            } else {
                break;
            }
        }
        Ok(s)
    }
    fn schema_name(&mut self) -> Result<String, crate::parser::ParseError> {
        let Some(Token::Word(word)) = self.v.get(self.i) else {
            return Err(parse_error());
        };
        if word.value.is_empty() || word.value.chars().count() > 64 {
            return Err(parse_error());
        }
        self.i += 1;
        Ok(word.value.to_ascii_lowercase())
    }
    fn model_name(&mut self) -> Result<String, crate::parser::ParseError> {
        let first = self.schema_name()?;
        let (schema, object) = if self.v.get(self.i) == Some(&Token::Period) {
            self.i += 1;
            (Some(first), self.schema_name()?)
        } else {
            (None, first)
        };
        if object.len() > 64
            || !object.bytes().enumerate().all(|(i, b)| {
                b == b'_' || b.is_ascii_alphanumeric() && (i > 0 || !b.is_ascii_digit())
            })
        {
            return Err(parse_error());
        }
        Ok(schema.map_or_else(|| object.clone(), |schema| format!("{schema}.{object}")))
    }
    fn literal(&mut self) -> Result<String, crate::parser::ParseError> {
        if let Some(Token::SingleQuotedString(v)) = self.v.get(self.i) {
            self.i += 1;
            Ok(v.clone())
        } else {
            Err(parse_error())
        }
    }
}
fn parse_error() -> crate::parser::ParseError {
    crate::parser::parse_expr_fragment("(").unwrap_err()
}
pub(crate) fn parse(sql: &str) -> Option<Result<Statement, crate::parser::ParseError>> {
    let trimmed = sql.trim_start();
    let keyword_len = trimmed.bytes().take_while(u8::is_ascii_alphabetic).count();
    let first = &trimmed[..keyword_len];
    if !["CREATE", "ALTER", "DROP", "SHOW", "GRANT", "REVOKE"]
        .iter()
        .any(|v| first.eq_ignore_ascii_case(v))
    {
        return None;
    }
    if !sql
        .as_bytes()
        .get(..sql.len().min(128))
        .unwrap_or_default()
        .windows(2)
        .any(|v| v.eq_ignore_ascii_case(b"AI"))
    {
        return None;
    }
    let dialect = crate::parser::MydbDialect::default();
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut tokens: Vec<_> = tokens
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_) | Token::EOF))
        .collect();
    if tokens.last() == Some(&Token::SemiColon) {
        tokens.pop();
    }
    let mut c = Tokens { v: tokens, i: 0 };
    let action = if c.eat("CREATE") {
        "create"
    } else if c.eat("ALTER") {
        "alter"
    } else if c.eat("DROP") {
        "drop"
    } else if c.eat("SHOW") {
        "show"
    } else if c.eat("GRANT") {
        "grant"
    } else if c.eat("REVOKE") {
        "revoke"
    } else {
        return None;
    };
    let mut a = AiStatement {
        action: action.into(),
        kind: String::new(),
        name: String::new(),
        connection: None,
        config: None,
        value: None,
        if_exists: false,
        tail: None,
    };
    if matches!(action, "grant" | "revoke") {
        if !c.eat("AI_USE") {
            return None;
        }
        if !c.eat("ON") {
            return None;
        }
        if !c.eat("AI") {
            return None;
        }
        a.kind = "model".into();
        let result = (|| {
            c.expect("MODEL")?;
            a.name = c.model_name()?;
            a.tail = Some(
                c.v[c.i..]
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" "),
            );
            Ok(Statement::Ai(a))
        })();
        return Some(result);
    }
    if action == "alter" && c.eat("SYSTEM") {
        if c.eat("RELOAD") {
            if !c.eat("AI") {
                return None;
            }
            a.action = "reload".into();
            return Some(c.expect("CONFIG").and_then(|_| {
                if c.i == c.v.len() {
                    Ok(Statement::Ai(a))
                } else {
                    Err(parse_error())
                }
            }));
        }
        if c.eat("ROTATE") && c.eat("AI") {
            a.action = "rotate".into();
            return Some(
                c.expect("ENCRYPTION")
                    .and_then(|_| c.expect("KEY"))
                    .and_then(|_| {
                        if c.i == c.v.len() {
                            Ok(Statement::Ai(a))
                        } else {
                            Err(parse_error())
                        }
                    }),
            );
        }
        return None;
    }
    if action == "show" && c.eat("CREATE") {
        a.action = "show_create".into();
    }
    if !c.eat("AI") {
        return None;
    }
    let result = (|| {
        a.kind = if c.eat("SECRET") || c.eat("SECRETS") {
            "secret"
        } else if c.eat("CONNECTION") || c.eat("CONNECTIONS") {
            "connection"
        } else if c.eat("MODEL") || c.eat("MODELS") {
            "model"
        } else {
            return Err(parse_error());
        }
        .into();
        if a.action == "show" {
            if c.eat("FROM") {
                a.name = c.schema_name()?;
            }
        } else {
            if c.eat("IF") {
                if action == "create" {
                    c.expect("NOT")?;
                }
                c.expect("EXISTS")?;
                a.if_exists = true;
            }
            a.name = if a.kind == "model" {
                c.model_name()?
            } else {
                c.name()?
            };
            if action == "create" && a.kind == "model" {
                c.expect("USING")?;
                a.connection = Some(c.name()?);
            }
            if matches!(action, "create" | "alter") {
                if a.kind == "secret" {
                    c.expect("VALUE")?;
                    a.value = Some(match c.v.get(c.i) {
                        Some(Token::Placeholder(p)) if p == "?" => {
                            c.i += 1;
                            Expr::Placeholder(0)
                        }
                        Some(Token::SingleQuotedString(_)) => {
                            Expr::Literal(crate::parser::Literal::String(c.literal()?))
                        }
                        _ => return Err(parse_error()),
                    });
                } else {
                    c.expect("WITH")?;
                    c.expect("CONFIG")?;
                    a.config = Some(c.literal()?);
                }
            }
        }
        if c.i != c.v.len() {
            return Err(parse_error());
        }
        Ok(Statement::Ai(a))
    })();
    Some(result)
}
pub(crate) fn statement_has_ai(statement: &Statement) -> bool {
    struct Find(bool);
    impl crate::parser::visit::AstVisitor for Find {
        fn enter_statement(&mut self, s: &Statement) {
            if matches!(s, Statement::Ai(_)) {
                self.0 = true;
            }
        }
        fn enter_expr(&mut self, e: &Expr) {
            if matches!(e,Expr::Function{name,..}if is_function(name)) {
                self.0 = true;
            }
        }
    }
    let mut f = Find(false);
    crate::parser::visit::visit_statement(&mut f, statement);
    f.0
}
pub(crate) fn expr_has_ai(expr: &Expr) -> bool {
    crate::parser::visit::any_expr(
        expr,
        |e| matches!(e,Expr::Function{name,..}if is_function(name)),
    )
}
/// Conservative raw-text routing also protects malformed sensitive statements.
pub(crate) fn sql_may_have_ai(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    for i in memchr::memchr2_iter(b'A', b'a', bytes) {
        if bytes
            .get(i + 1)
            .is_none_or(|b| !b.eq_ignore_ascii_case(&b'I'))
            || (i > 0 && word(bytes[i - 1]))
        {
            continue;
        }
        for name in [
            b"AI_GENERATE_DETAIL".as_slice(),
            b"AI_GENERATE",
            b"AI_EMBED_DETAIL",
            b"AI_EMBED",
        ] {
            if bytes
                .get(i..i + name.len())
                .is_some_and(|s| s.eq_ignore_ascii_case(name))
            {
                let mut end = i + name.len();
                if i > 0
                    && matches!(bytes[i - 1], b'`' | b'"')
                    && bytes.get(end) == Some(&bytes[i - 1])
                {
                    end += 1;
                }
                while bytes.get(end).is_some_and(u8::is_ascii_whitespace) {
                    end += 1;
                }
                if bytes.get(end) == Some(&b'(')
                    || bytes.get(end..end + 2) == Some(b"/*")
                    || bytes.get(end..end + 2) == Some(b"--")
                    || bytes.get(end) == Some(&b'#')
                {
                    return true;
                }
            }
        }
        if bytes.get(i + 2).is_some_and(|b| word(*b) || *b == b'.') {
            continue;
        }
        let mut end = i + 2;
        if i > 0 && matches!(bytes[i - 1], b'`' | b'"') && bytes.get(end) == Some(&bytes[i - 1]) {
            end += 1;
        }
        while bytes.get(end).is_some_and(u8::is_ascii_whitespace) {
            end += 1;
        }
        if bytes.get(end..end + 2) == Some(b"/*")
            || bytes.get(end..end + 2) == Some(b"--")
            || bytes.get(end) == Some(&b'#')
        {
            return true;
        }
        let start = end;
        while bytes.get(end).is_some_and(|b| word(*b)) {
            end += 1;
        }
        if [
            b"SECRET".as_slice(),
            b"SECRETS",
            b"MODEL",
            b"MODELS",
            b"CONNECTION",
            b"CONNECTIONS",
            b"CONFIG",
            b"ENCRYPTION",
        ]
        .iter()
        .any(|name| bytes[start..end].eq_ignore_ascii_case(name))
        {
            return true;
        }
        let prefix = sql[..i].trim_end();
        let start = prefix
            .as_bytes()
            .iter()
            .rposition(|b| !b.is_ascii_alphabetic())
            .map_or(0, |i| i + 1);
        if ["CREATE", "ALTER", "DROP", "SHOW"]
            .iter()
            .any(|name| prefix[start..].eq_ignore_ascii_case(name))
        {
            return true;
        }
    }
    false
}
