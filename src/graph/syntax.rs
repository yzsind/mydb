use super::*;
use crate::parser::{Literal, ParseError};

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Word(String),
    Identifier(String),
    String(String),
    Integer(String),
    Float(String),
    Symbol(char),
    End,
}

fn word(input: &str) -> Option<&str> {
    let input = input.trim_start();
    let n = input
        .bytes()
        .take_while(|c| c.is_ascii_alphabetic() || *c == b'_')
        .count();
    (n != 0).then(|| &input[..n])
}

// The SQL and GQL entry points share INSERT, CREATE, DROP, and SET.
// Recognize only the GQL forms before dispatching to the GQL lexer.
pub(crate) fn parse(sql: &str, head: Option<&[u8]>) -> Option<Result<Command>> {
    let head = head?;
    let rest = sql.get(head.len()..)?.trim_start();
    let next = word(rest).unwrap_or_default();
    let first = |name: &[u8]| head.eq_ignore_ascii_case(name);
    let second = |name: &str| next.eq_ignore_ascii_case(name);
    let graph_ddl = (first(b"CREATE") || first(b"DROP"))
        && (second("GRAPH")
            || second("PROPERTY")
            || (first(b"CREATE")
                && second("UNIQUE")
                && word(&rest[next.len()..]).is_some_and(|w| w.eq_ignore_ascii_case("GRAPH"))));
    if graph_ddl
        || first(b"MATCH")
        || first(b"OPTIONAL")
        || first(b"FOR")
        || first(b"SESSION")
        || (first(b"INSERT") && rest.starts_with('('))
    {
        Some(Parser::new(sql).and_then(Parser::command))
    } else {
        None
    }
}

fn parse_error(sql: &str, offset: usize, message: impl Into<String>) -> ParseError {
    let before = &sql[..offset];
    ParseError {
        message: message.into(),
        span: crate::parser::Span {
            start: offset,
            end: offset,
        },
        line: before.bytes().filter(|c| *c == b'\n').count() + 1,
        column: before
            .rsplit('\n')
            .next()
            .unwrap_or_default()
            .chars()
            .count()
            + 1,
        ..ParseError::at_zero("")
    }
}

fn lex(sql: &str) -> Result<Vec<(usize, Token)>> {
    let mut chars = sql.char_indices().peekable();
    let mut tokens = Vec::new();
    while let Some((offset, ch)) = chars.next() {
        let token = match ch {
            c if c.is_whitespace() => continue,
            '/' if chars.next_if(|&(_, c)| c == '/').is_some() => {
                while chars.next_if(|&(_, c)| c != '\n').is_some() {}
                continue;
            }
            '/' if chars.next_if(|&(_, c)| c == '*').is_some() => {
                let mut closed = false;
                while let Some((_, c)) = chars.next() {
                    if c == '*' && chars.next_if(|&(_, c)| c == '/').is_some() {
                        closed = true;
                        break;
                    }
                }
                if !closed {
                    return Err(parse_error(sql, offset, "Unterminated GQL comment"));
                }
                continue;
            }
            '\'' | '"' | '`' => {
                let mut value = String::new();
                loop {
                    match chars.next() {
                        Some((_, c)) if c == ch => {
                            if chars.next_if(|&(_, c)| c == ch).is_none() {
                                break;
                            }
                            value.push(ch);
                        }
                        Some((_, c)) => value.push(c),
                        None => {
                            return Err(parse_error(sql, offset, "Unterminated quoted GQL value"))
                        }
                    }
                }
                if ch == '\'' {
                    Token::String(value)
                } else {
                    Token::Identifier(value)
                }
            }
            c if c.is_ascii_digit() => {
                while chars.next_if(|&(_, c)| c.is_ascii_digit()).is_some() {}
                let fraction = chars.next_if(|&(_, c)| c == '.').is_some();
                if fraction {
                    while chars.next_if(|&(_, c)| c.is_ascii_digit()).is_some() {}
                }
                let exponent = chars
                    .next_if(|&(_, c)| c.eq_ignore_ascii_case(&'e'))
                    .is_some();
                if exponent {
                    chars.next_if(|&(_, c)| c == '+' || c == '-');
                    if chars.next_if(|&(_, c)| c.is_ascii_digit()).is_none() {
                        return Err(parse_error(sql, offset, "Invalid GQL number"));
                    }
                    while chars.next_if(|&(_, c)| c.is_ascii_digit()).is_some() {}
                }
                let end = chars.peek().map_or(sql.len(), |&(i, _)| i);
                if fraction || exponent {
                    Token::Float(sql[offset..end].into())
                } else {
                    Token::Integer(sql[offset..end].into())
                }
            }
            c if c.is_alphabetic() || c == '_' => {
                while chars
                    .next_if(|&(_, c)| c.is_alphanumeric() || c == '_')
                    .is_some()
                {}
                let end = chars.peek().map_or(sql.len(), |&(i, _)| i);
                Token::Word(sql[offset..end].into())
            }
            c if "()[]{}.,:?;=<>!+~-&".contains(c) => Token::Symbol(c),
            _ => {
                return Err(parse_error(
                    sql,
                    offset,
                    format!("Unsupported GQL token {ch:?}"),
                ))
            }
        };
        tokens.push((offset, token));
    }
    tokens.push((sql.len(), Token::End));
    Ok(tokens)
}

type Result<T> = std::result::Result<T, ParseError>;

struct Parser<'s> {
    sql: &'s str,
    tokens: Vec<(usize, Token)>,
    pos: usize,
    atoms: Vec<Expr>,
    depth: usize,
    placeholders: usize,
}

impl<'s> Parser<'s> {
    fn new(sql: &'s str) -> Result<Self> {
        Ok(Self {
            sql,
            tokens: lex(sql)?,
            pos: 0,
            atoms: Vec::new(),
            depth: 0,
            placeholders: 0,
        })
    }

    fn command(mut self) -> Result<Command> {
        let command = if self.eat("CREATE") {
            let unique = self.eat("UNIQUE");
            self.eat("PROPERTY");
            self.require("GRAPH")?;
            if self.eat("INDEX") {
                let name = self.name()?;
                self.require("ON")?;
                let graph = self.name()?;
                self.require("NODE")?;
                self.require("LABEL")?;
                let label = self.name()?;
                self.require_symbol('(')?;
                let property = self.name()?;
                let kind = if self.eat("STRING") {
                    IndexType::String
                } else {
                    self.require("INT64")?;
                    IndexType::Int64
                };
                self.require_symbol(')')?;
                Command::CreateIndex {
                    graph,
                    name,
                    spec: PropertyIndex {
                        label,
                        property,
                        kind,
                        unique,
                    },
                }
            } else {
                if unique {
                    return self.err("Expected GRAPH INDEX after CREATE UNIQUE");
                }
                let conditional = self.conditional(true)?;
                let graph = self.name()?;
                self.eat("TYPED");
                self.require("ANY")?;
                if self.eat("PROPERTY") {
                    self.require("GRAPH")?;
                } else {
                    self.eat("GRAPH");
                }
                Command::Create { graph, conditional }
            }
        } else if self.eat("DROP") {
            self.eat("PROPERTY");
            self.require("GRAPH")?;
            if self.eat("INDEX") {
                let name = self.name()?;
                self.require("ON")?;
                Command::DropIndex {
                    graph: self.name()?,
                    name,
                }
            } else {
                let conditional = self.conditional(false)?;
                Command::Drop {
                    graph: self.name()?,
                    conditional,
                }
            }
        } else if self.eat("SESSION") {
            if self.eat("SET") {
                self.eat("PROPERTY");
                self.require("GRAPH")?;
                Command::SetGraph(self.name()?)
            } else {
                self.require("RESET")?;
                self.eat("PROPERTY");
                self.require("GRAPH")?;
                Command::ResetGraph
            }
        } else {
            Command::Program(self.program()?)
        };
        self.symbol(';');
        if self.token() != &Token::End {
            return self.err("Unsupported or unexpected GQL clause");
        }
        Ok(command)
    }

    fn conditional(&mut self, create: bool) -> Result<bool> {
        if !self.eat("IF") {
            return Ok(false);
        }
        if create {
            self.require("NOT")?;
        }
        self.require("EXISTS")?;
        Ok(true)
    }

    fn program(&mut self) -> Result<Program> {
        let mut clauses = Vec::new();
        loop {
            let optional = self.eat("OPTIONAL");
            if optional || self.at("MATCH") {
                self.require("MATCH")?;
                let patterns = self.patterns(false)?;
                let predicate = if self.eat("WHERE") {
                    Some(self.expression(0)?)
                } else {
                    None
                };
                clauses.push(Clause::Match {
                    optional,
                    patterns,
                    predicate,
                });
            } else if self.eat("INSERT") {
                clauses.push(Clause::Insert(self.patterns(true)?));
            } else if self.eat("FOR") {
                let key = self.name()?;
                self.require("IN")?;
                self.require_symbol('[')?;
                let values = if self.at_symbol(']') {
                    Vec::new()
                } else {
                    self.list(|p| p.expression(0))?
                };
                self.require_symbol(']')?;
                clauses.push(Clause::For(key, values));
            } else if self.eat("SET") {
                clauses.push(Clause::Set(self.list(|p| p.change(true))?));
            } else if self.eat("REMOVE") {
                clauses.push(Clause::Remove(self.list(|p| p.change(false))?));
            } else if self.at("DELETE") || self.at("DETACH") || self.at("NODETACH") {
                let detach = self.eat("DETACH");
                if !detach {
                    self.eat("NODETACH");
                }
                self.require("DELETE")?;
                clauses.push(Clause::Delete {
                    detach,
                    variables: self.list(Parser::name)?,
                });
            } else {
                break;
            }
        }
        let result = if self.eat("RETURN") {
            let distinct = self.eat("DISTINCT");
            if !distinct {
                self.eat("ALL");
            }
            let items = self.list(|p| {
                let start = p.tokens[p.pos].0;
                let expression = p.expression(0)?;
                let alias = if p.eat("AS") {
                    p.name()?
                } else {
                    p.sql[start..p.tokens[p.pos].0].trim().into()
                };
                Ok((expression, alias))
            })?;
            let order = if self.eat("ORDER") {
                self.require("BY")?;
                self.list(|p| {
                    let expr = p.expression(0)?;
                    let desc = p.eat("DESC") || p.eat("DESCENDING");
                    if !desc && !p.eat("ASC") {
                        p.eat("ASCENDING");
                    }
                    Ok((expr, desc))
                })?
            } else {
                Vec::new()
            };
            let limit = if self.eat("LIMIT") {
                Some(self.expression(0)?)
            } else {
                None
            };
            Some(Projection {
                distinct,
                items,
                order,
                limit,
            })
        } else {
            self.eat("FINISH");
            None
        };
        let program = Program {
            clauses,
            result,
            atoms: std::mem::take(&mut self.atoms),
        };
        if program.clauses.is_empty() || (!program.writes() && program.result.is_none()) {
            return self.err("GQL query requires clauses and a RETURN projection");
        }
        super::binding::validate(&program)
            .map_err(|message| parse_error(self.sql, self.tokens[self.pos].0, message))?;
        Ok(program)
    }

    fn change(&mut self, set: bool) -> Result<Change> {
        let variable = self.name()?;
        if self.symbol('.') {
            let key = self.name()?;
            let value = if set {
                self.require_symbol('=')?;
                Some(self.expression(0)?)
            } else {
                None
            };
            Ok(Change::Property {
                variable,
                key,
                value,
            })
        } else {
            if !self.symbol(':') {
                self.require("IS")?;
            }
            Ok(Change::Label {
                variable,
                label: self.name()?,
            })
        }
    }

    fn patterns(&mut self, insert: bool) -> Result<Vec<Pattern>> {
        self.list(|p| p.pattern(insert))
    }

    fn pattern(&mut self, insert: bool) -> Result<Pattern> {
        let path = if !insert
            && matches!(self.token(), Token::Word(_) | Token::Identifier(_))
            && self
                .tokens
                .get(self.pos + 1)
                .is_some_and(|(_, tok)| tok == &Token::Symbol('='))
        {
            let variable = self.name()?;
            self.require_symbol('=')?;
            Some(variable)
        } else {
            None
        };
        let shortest = !insert && self.eat("ANY");
        if shortest {
            self.require("SHORTEST")?;
        }
        let mode = if !insert && self.eat("TRAIL") {
            PathMode::Trail
        } else if !insert && self.eat("ACYCLIC") {
            PathMode::Acyclic
        } else {
            if !insert {
                self.eat("WALK");
            }
            PathMode::Walk
        };
        if !insert && !self.eat("PATH") {
            self.eat("PATHS");
        }
        let start = self.element('(', ')', insert)?;
        let mut steps = Vec::new();
        let mut max_hops = 0u16;
        while self.at_symbol('-') || self.at_symbol('<') || self.at_symbol('~') {
            let left = self.symbol('<');
            let undirected = !left && self.symbol('~');
            if !undirected {
                self.require_symbol('-')?;
            }
            let explicit = self.at_symbol('[');
            let edge = if explicit {
                self.element('[', ']', insert)?
            } else {
                Element::default()
            };
            if explicit {
                self.require_symbol(if undirected { '~' } else { '-' })?;
            }
            let right = !undirected && !left && self.symbol('>');
            let direction = if left {
                Direction::In
            } else if right {
                Direction::Out
            } else if undirected {
                Direction::Undirected
            } else {
                Direction::Either
            };
            if insert && direction == Direction::Either {
                return self.err("INSERT requires a directed or undirected edge");
            }
            let quantified = !insert && self.symbol('{');
            let (lower, upper) = if quantified {
                let lower = if self.at_symbol(',') { 0 } else { self.hops()? };
                let upper = if self.symbol(',') {
                    self.hops()?
                } else {
                    lower
                };
                self.require_symbol('}')?;
                (lower, upper)
            } else {
                (1, 1)
            };
            max_hops += u16::from(upper);
            if lower > upper || max_hops > u16::from(MAX_HOPS) {
                return self.err(format!("Invalid GQL path bound (maximum {MAX_HOPS} hops)"));
            }
            let node = self.element('(', ')', insert)?;
            steps.push(Step {
                edge,
                direction,
                lower,
                upper,
                quantified,
                node,
            });
        }
        if shortest && steps.is_empty() {
            return self.err("ANY SHORTEST requires an edge pattern");
        }
        Ok(Pattern {
            path,
            mode,
            shortest,
            start,
            steps,
        })
    }

    fn element(&mut self, open: char, close: char, insert: bool) -> Result<Element> {
        self.require_symbol(open)?;
        let variable = if !self.at("IS")
            && !self.at("WHERE")
            && matches!(self.token(), Token::Word(_) | Token::Identifier(_))
        {
            Some(self.name()?)
        } else {
            None
        };
        let labels = if self.symbol(':') || self.eat("IS") {
            let mut labels = vec![self.name()?];
            while self.symbol('&') {
                labels.push(self.name()?);
            }
            labels
        } else {
            Vec::new()
        };
        let properties = if self.symbol('{') {
            let properties = self.list(|p| {
                let name = p.name()?;
                p.require_symbol(':')?;
                Ok((name, p.expression(0)?))
            })?;
            self.require_symbol('}')?;
            properties
        } else {
            Vec::new()
        };
        let predicate = if !insert && self.eat("WHERE") {
            Some(self.expression(0)?)
        } else {
            None
        };
        self.require_symbol(close)?;
        Ok(Element {
            variable,
            labels,
            properties,
            predicate,
        })
    }

    fn expression(&mut self, min: u8) -> Result<Expression> {
        self.depth += 1;
        if self.depth > 64 {
            return self.err("GQL expression is too deeply nested");
        }
        let result = self.expression_inner(min);
        self.depth -= 1;
        result
    }

    fn expression_inner(&mut self, min: u8) -> Result<Expression> {
        let mut left = if self.eat("NOT") {
            Expression::Not(Box::new(self.expression(3)?))
        } else {
            self.primary()?
        };
        let mut operators = 0;
        loop {
            if operators == 64 {
                return self.err("GQL expression contains too many chained operators");
            }
            if min <= 3 && self.eat("IS") {
                operators += 1;
                let not = self.eat("NOT");
                self.require("NULL")?;
                left = Expression::IsNull(Box::new(left), not);
                continue;
            }
            let (prec, op) = match self.token() {
                Token::Word(word) if word.eq_ignore_ascii_case("OR") => (1, Operator::Or),
                Token::Word(word) if word.eq_ignore_ascii_case("AND") => (2, Operator::And),
                Token::Symbol('=') => (3, Operator::Eq),
                Token::Symbol('<') => (3, Operator::Lt),
                Token::Symbol('>') => (3, Operator::Gt),
                Token::Symbol('!') => (3, Operator::Ne),
                _ => break,
            };
            if prec < min {
                break;
            }
            self.pos += 1;
            operators += 1;
            let operator = match op {
                Operator::Lt if self.symbol('=') => Operator::Le,
                Operator::Lt if self.symbol('>') => Operator::Ne,
                Operator::Gt if self.symbol('=') => Operator::Ge,
                Operator::Ne => {
                    self.require_symbol('=')?;
                    Operator::Ne
                }
                _ => op,
            };
            left = Expression::Binary(
                Box::new(left),
                operator,
                Box::new(self.expression(prec + 1)?),
            );
        }
        Ok(left)
    }

    fn primary(&mut self) -> Result<Expression> {
        if self.symbol('{') {
            let fields = self.list(|parser| {
                let name = parser.name()?;
                parser.require_symbol(':')?;
                Ok((name, parser.expression(0)?))
            })?;
            self.require_symbol('}')?;
            return Ok(Expression::Record(fields));
        }
        if self.symbol('(') {
            let expr = self.expression(0)?;
            self.require_symbol(')')?;
            return Ok(expr);
        }
        if self.eat("CAST") {
            self.require_symbol('(')?;
            let expr = self.expression(0)?;
            self.require("AS")?;
            let kind = if self.eat("STRING") {
                Cast::String
            } else if self.eat("INT64") {
                Cast::Int64
            } else if self.eat("FLOAT64") {
                Cast::Float64
            } else {
                self.require("BOOLEAN")?;
                Cast::Boolean
            };
            self.require_symbol(')')?;
            return Ok(Expression::Cast(Box::new(expr), kind));
        }
        if self.at("ELEMENT_ID") || self.at("PATH_LENGTH") {
            let id = self.eat("ELEMENT_ID");
            if !id {
                self.require("PATH_LENGTH")?;
            }
            self.require_symbol('(')?;
            let name = self.name()?;
            self.require_symbol(')')?;
            return Ok(if id {
                Expression::ElementId(name)
            } else {
                Expression::PathLength(name)
            });
        }
        if self.symbol('?') {
            let index = self.placeholders;
            self.placeholders += 1;
            return Ok(self.atom(Expr::Placeholder(index)));
        }
        let sign = if self.symbol('-') {
            "-"
        } else if self.symbol('+') {
            "+"
        } else {
            ""
        };
        let literal = match self.token() {
            Token::String(value) if sign.is_empty() => Literal::String(value.clone()),
            Token::Integer(text) => {
                let number = format!("{sign}{text}");
                if let Ok(value) = number.parse() {
                    Literal::Int(value)
                } else if let Ok(value) = number.parse() {
                    Literal::UInt(value)
                } else {
                    return self.err("GQL integer is out of range");
                }
            }
            Token::Float(text) => {
                let value = format!("{sign}{text}")
                    .parse::<f64>()
                    .ok()
                    .filter(|n| n.is_finite());
                Literal::Float(value.ok_or_else(|| {
                    parse_error(
                        self.sql,
                        self.tokens[self.pos].0,
                        "GQL float is out of range",
                    )
                })?)
            }
            Token::Word(word) if sign.is_empty() && word.eq_ignore_ascii_case("NULL") => {
                Literal::Null
            }
            Token::Word(word) if sign.is_empty() && word.eq_ignore_ascii_case("TRUE") => {
                Literal::Bool(true)
            }
            Token::Word(word) if sign.is_empty() && word.eq_ignore_ascii_case("FALSE") => {
                Literal::Bool(false)
            }
            _ if !sign.is_empty() => return self.err("Expected a GQL number"),
            _ => {
                let var = self.name()?;
                return if self.symbol('.') {
                    Ok(Expression::Property(var, self.name()?))
                } else {
                    Ok(Expression::Variable(var))
                };
            }
        };
        self.pos += 1;
        Ok(self.atom(Expr::Literal(literal)))
    }

    fn atom(&mut self, atom: Expr) -> Expression {
        let index = self.atoms.len();
        self.atoms.push(atom);
        Expression::Atom(index)
    }

    fn hops(&mut self) -> Result<u8> {
        if let Token::Integer(text) = self.token() {
            if let Ok(value) = text.parse() {
                self.pos += 1;
                return Ok(value);
            }
        }
        self.err("Expected a finite GQL hop bound")
    }

    fn list<T>(&mut self, mut parse: impl FnMut(&mut Self) -> Result<T>) -> Result<Vec<T>> {
        let mut result = vec![parse(self)?];
        while self.symbol(',') {
            result.push(parse(self)?);
        }
        Ok(result)
    }

    fn token(&self) -> &Token {
        &self.tokens[self.pos].1
    }
    fn at(&self, keyword: &str) -> bool {
        matches!(self.token(), Token::Word(word) if word.eq_ignore_ascii_case(keyword))
    }
    fn eat(&mut self, keyword: &str) -> bool {
        let found = self.at(keyword);
        self.pos += usize::from(found);
        found
    }
    fn require(&mut self, keyword: &str) -> Result<()> {
        if self.eat(keyword) {
            Ok(())
        } else {
            self.err(format!("Expected {keyword} in GQL statement"))
        }
    }
    fn at_symbol(&self, ch: char) -> bool {
        self.token() == &Token::Symbol(ch)
    }
    fn symbol(&mut self, ch: char) -> bool {
        let found = self.at_symbol(ch);
        self.pos += usize::from(found);
        found
    }
    fn require_symbol(&mut self, ch: char) -> Result<()> {
        if self.symbol(ch) {
            Ok(())
        } else {
            self.err(format!("Expected '{ch}' in GQL statement"))
        }
    }
    fn name(&mut self) -> Result<String> {
        match self.token() {
            Token::Word(word) | Token::Identifier(word) if !word.is_empty() => {
                let value = word.clone();
                self.pos += 1;
                Ok(value)
            }
            _ => self.err("Expected GQL identifier"),
        }
    }
    fn err<T>(&self, message: impl Into<String>) -> Result<T> {
        Err(parse_error(self.sql, self.tokens[self.pos].0, message))
    }
}
