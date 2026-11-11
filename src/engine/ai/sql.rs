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
struct Tokens<'a> {
    sql: &'a str,
    v: Vec<Token>,
    i: usize,
    positions: Vec<(usize, usize)>,
    end: (usize, usize),
}
impl Tokens<'_> {
    fn error(&self) -> crate::parser::ParseError {
        let mut error = crate::parser::ParseError::at_zero("invalid AI statement syntax");
        (error.line, error.column) = self.positions.get(self.i).copied().unwrap_or(self.end);
        let (mut line, mut column) = (1, 1);
        let mut offset = self.sql.len();
        for (i, ch) in self.sql.char_indices() {
            if (line, column) == (error.line, error.column) {
                offset = i;
                break;
            }
            if ch == '\n' {
                line += 1;
                column = 1;
            } else {
                column += 1;
            }
        }
        error.span = crate::parser::Span {
            start: offset,
            end: offset,
        };
        error
    }
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
            Err(self.error())
        }
    }
    fn name(&mut self) -> Result<String, crate::parser::ParseError> {
        let mut s = String::new();
        loop {
            let Some(Token::Word(w)) = self.v.get(self.i) else {
                return Err(self.error());
            };
            if w.value.is_empty()
                || w.value.len() > 64
                || !w.value.bytes().enumerate().all(|(i, b)| {
                    b.is_ascii_alphanumeric() && (i > 0 || !b.is_ascii_digit()) || b == b'_'
                })
            {
                return Err(self.error());
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
            return Err(self.error());
        };
        if word.value.is_empty() || word.value.chars().count() > 64 {
            return Err(self.error());
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
            return Err(self.error());
        }
        Ok(schema.map_or_else(|| object.clone(), |schema| format!("{schema}.{object}")))
    }
    fn literal(&mut self) -> Result<String, crate::parser::ParseError> {
        if let Some(Token::SingleQuotedString(v)) = self.v.get(self.i) {
            self.i += 1;
            Ok(v.clone())
        } else {
            Err(self.error())
        }
    }
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
    let located = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .ok()?;
    let end = located.last().map_or((1, 1), |t| {
        (t.span.end.line as usize, t.span.end.column as usize)
    });
    let (mut tokens, mut positions): (Vec<_>, Vec<_>) = located
        .into_iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_) | Token::EOF))
        .map(|t| {
            (
                t.token,
                (t.span.start.line as usize, t.span.start.column as usize),
            )
        })
        .unzip();
    if tokens.last() == Some(&Token::SemiColon) {
        tokens.pop();
        positions.pop();
    }
    let mut c = Tokens {
        sql,
        v: tokens,
        i: 0,
        positions,
        end,
    };
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
            // Keep original quoting and escaping. Token Display implementations
            // are not SQL serializers for decoded account names.
            let mut line = 1usize;
            let mut column = 1usize;
            let position = c.positions.get(c.i).copied().unwrap_or(c.end);
            let offset = sql
                .char_indices()
                .find_map(|(offset, ch)| {
                    if (line, column) == position {
                        return Some(offset);
                    }
                    if ch == '\n' {
                        line += 1;
                        column = 1;
                    } else {
                        column += 1;
                    }
                    None
                })
                .unwrap_or(sql.len());
            a.tail = Some(sql[offset..].to_owned());
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
                    Err(c.error())
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
                            Err(c.error())
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
        a.kind = if c.eat("SECRET") || (a.action == "show" && c.eat("SECRETS")) {
            "secret"
        } else if c.eat("CONNECTION") || (a.action == "show" && c.eat("CONNECTIONS")) {
            "connection"
        } else if c.eat("MODEL") || (a.action == "show" && c.eat("MODELS")) {
            "model"
        } else {
            return Err(c.error());
        }
        .into();
        if a.action == "show" {
            if c.eat("FROM") {
                if a.kind != "model" {
                    return Err(c.error());
                }
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
                        _ => return Err(c.error()),
                    });
                } else {
                    c.expect("WITH")?;
                    c.expect("CONFIG")?;
                    a.config = Some(c.literal()?);
                }
            }
        }
        if c.i != c.v.len() {
            return Err(c.error());
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
    let has_index_call = |expressions: &[Option<String>]| {
        expressions
            .iter()
            .flatten()
            .any(|sql| crate::parser::parse_expr_fragment(sql).is_ok_and(|expr| expr_has_ai(&expr)))
    };
    let index_call = match statement {
        Statement::CreateTableAsSelect { table, .. } => table.indexes.iter().any(|index| has_index_call(&index.key_expressions)),
        Statement::CreateTable(table) => table.indexes.iter().any(|index| has_index_call(&index.key_expressions)),
        Statement::CreateIndex(index) => has_index_call(&index.key_expressions),
        Statement::AlterTable(table) => table.actions.iter().any(|action| matches!(action, crate::parser::AlterAction::AddIndex(index) if has_index_call(&index.key_expressions))),
        _ => false,
    };
    if index_call {
        return true;
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
fn ai_tokens_sensitive(sql: &str) -> bool {
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

/// Inspect executable SQL, excluding literal data and ordinary comments.
pub(crate) fn sql_calls_ai(sql: &str, mode: crate::sql_lex::SqlLexMode) -> bool {
    use crate::sql_lex::{segments, SqlSegmentKind};
    if !ai_tokens_sensitive(sql) && !sql.contains("/*!") {
        return false;
    }
    let mut code = String::with_capacity(sql.len().min(4096));
    for segment in segments(sql, mode, u32::MAX) {
        let Ok(segment) = segment else {
            return false;
        };
        match segment.kind {
            SqlSegmentKind::Code => code.push_str(&sql[segment.range]),
            SqlSegmentKind::ExecutableComment { body, .. } => code.push_str(&sql[body]),
            SqlSegmentKind::Quoted { quote: b'`' } => code.push_str(&sql[segment.range]),
            SqlSegmentKind::Quoted { quote: b'"' } if mode.ansi_quotes => {
                code.push_str(&sql[segment.range])
            }
            _ => code.push(' '),
        }
    }
    ai_tokens_sensitive(&code)
}

pub(crate) fn sql_may_have_ai(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    // Common malformed credential statements are still sensitive before parse.
    let mut words = trimmed.split_whitespace();
    if let (Some(first), Some(second)) = (words.next(), words.next()) {
        if ["CREATE", "ALTER"]
            .iter()
            .any(|v| first.eq_ignore_ascii_case(v))
            && ["AI_SECRET", "AISECRET", "SECRET"]
                .iter()
                .any(|v| second.eq_ignore_ascii_case(v))
        {
            return true;
        }
    }
    if !ai_tokens_sensitive(sql) && !sql.contains("/*!") {
        return false;
    }
    // Credential-bearing SQL remains sensitive when stored in a string, even
    // if the nested statement is misspelled. This does not affect execution routing.
    for segment in crate::sql_lex::segments(sql, Default::default(), u32::MAX).flatten() {
        if matches!(
            segment.kind,
            crate::sql_lex::SqlSegmentKind::Quoted {
                quote: b'\'' | b'"'
            }
        ) {
            let literal = &sql[segment.range];
            let upper = literal.to_ascii_uppercase();
            if (upper.contains("CREATE") || upper.contains("ALTER"))
                && upper.contains("AI")
                && upper.contains("SECR")
            {
                return true;
            }
        }
    }
    [false, true].into_iter().any(|no_backslash_escapes| {
        sql_calls_ai(
            sql,
            crate::sql_lex::SqlLexMode {
                no_backslash_escapes,
                ..Default::default()
            },
        )
    })
}
