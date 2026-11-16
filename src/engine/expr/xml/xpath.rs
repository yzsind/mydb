//! MySQL XPath grammar and immutable expression plans.
use crate::parser::BinaryOp;
use crate::types::{QueryError, Value};

#[derive(Debug, Clone, Copy)]
pub(super) enum Axis {
    Identity,
    Child,
    Attribute,
    SelfNode,
    Parent,
    Descendant,
    DescendantSelf,
    Ancestor,
    AncestorSelf,
}
#[derive(Debug)]
pub(super) struct Step {
    pub axis: Axis,
    pub name: Vec<u8>,
    pub predicates: Vec<XExpr>,
}
#[derive(Debug)]
pub(super) enum XExpr {
    Literal(Value),
    Real(f64, u8),
    Variable {
        name: String,
        user: bool,
        offset: usize,
    },
    Path {
        base: Option<Box<XExpr>>,
        root: bool,
        steps: Vec<Step>,
    },
    Call {
        name: String,
        args: Vec<XExpr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<XExpr>,
        right: Box<XExpr>,
        decimals: Option<u8>,
    },
    Union(Box<XExpr>, Box<XExpr>),
    Neg(Box<XExpr>),
}
impl XExpr {
    pub fn nodeset(&self) -> bool {
        matches!(self, Self::Path { .. } | Self::Union(..))
    }
    pub fn boolean(&self) -> bool {
        match self {
            Self::Binary { op, .. } => matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
                    | BinaryOp::And
                    | BinaryOp::Or
            ),
            Self::Call { name, .. } => matches!(
                name.as_str(),
                "true" | "false" | "not" | "boolean" | "contains"
            ),
            _ => false,
        }
    }
    pub fn decimals(&self) -> Option<u8> {
        match self {
            Self::Real(_, d) => Some(*d),
            Self::Literal(Value::Int(_) | Value::UInt(_)) => Some(0),
            Self::Neg(e) => e.decimals(),
            Self::Binary { decimals, .. } => *decimals,
            Self::Call { name, .. }
                if matches!(
                    name.as_str(),
                    "count"
                        | "position"
                        | "last"
                        | "floor"
                        | "ceiling"
                        | "round"
                        | "true"
                        | "false"
                        | "boolean"
                        | "not"
                        | "contains"
                        | "string-length"
                ) =>
            {
                Some(0)
            }
            _ => None,
        }
    }
    fn children<'a>(&'a self, pending: &mut Vec<&'a XExpr>) {
        match self {
            Self::Path { base, steps, .. } => {
                if let Some(base) = base {
                    pending.push(base);
                }
                for step in steps {
                    pending.extend(step.predicates.iter());
                }
            }
            Self::Call { args, .. } => pending.extend(args.iter()),
            Self::Binary { left, right, .. } | Self::Union(left, right) => {
                pending.push(left);
                pending.push(right);
            }
            Self::Neg(e) => pending.push(e),
            _ => {}
        }
    }
}
/// Cached syntax owns variable names, never their session values or local slots.
#[derive(Debug)]
pub(super) struct Program {
    pub expression: XExpr,
    pub variables: Vec<(String, bool, usize)>,
    pub work_vectors: usize,
}
impl Drop for XExpr {
    fn drop(&mut self) {
        fn detach(e: &mut XExpr, pending: &mut Vec<XExpr>) {
            match e {
                XExpr::Path { base, steps, .. } => {
                    if let Some(base) = base.take() {
                        pending.push(*base);
                    }
                    for step in steps {
                        pending.append(&mut step.predicates);
                    }
                }
                XExpr::Call { args, .. } => pending.append(args),
                XExpr::Binary { left, right, .. } | XExpr::Union(left, right) => {
                    pending.push(*std::mem::replace(
                        left,
                        Box::new(XExpr::Literal(Value::Null)),
                    ));
                    pending.push(*std::mem::replace(
                        right,
                        Box::new(XExpr::Literal(Value::Null)),
                    ));
                }
                XExpr::Neg(child) => pending.push(*std::mem::replace(
                    child,
                    Box::new(XExpr::Literal(Value::Null)),
                )),
                _ => {}
            }
        }
        if matches!(self, Self::Literal(_) | Self::Variable { .. }) {
            return;
        }
        let mut pending = Vec::new();
        detach(self, &mut pending);
        while let Some(mut child) = pending.pop() {
            detach(&mut child, &mut pending);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Name,
    Number,
    String,
    Symbol(u8),
    End,
}
#[derive(Debug, Clone, Copy)]
struct Token {
    kind: Kind,
    start: usize,
    end: usize,
}
struct Parser<'a> {
    source: &'a [u8],
    charset: &'static str,
    tokens: Vec<Token>,
    at: usize,
    depth: usize,
    in_predicate: usize,
}
fn name_character(s: &[u8], at: usize, charset: &str, first: bool) -> usize {
    let byte = s[at];
    if byte < 128 {
        return usize::from(if first {
            byte.is_ascii_alphabetic() || byte == b'_'
        } else {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
        });
    }
    let max = if byte & 0xe0 == 0xc0 {
        2
    } else if byte & 0xf0 == 0xe0 {
        3
    } else if byte & 0xf8 == 0xf0 {
        4
    } else {
        1
    };
    let (character, length) = if matches!(charset, "utf8mb4" | "utf8mb3" | "utf8") {
        if max > 3 || at + max > s.len() {
            return 0;
        }
        let Ok(text) = std::str::from_utf8(&s[at..at + max]) else {
            return 0;
        };
        (text.chars().next().unwrap(), max)
    } else if matches!(charset, "latin1" | "binary") {
        (char::from(byte), 1)
    } else {
        let mut found = None;
        for length in 1..=4.min(s.len() - at) {
            if let Ok(text) = crate::charset::decode_bytes(charset, &s[at..at + length]) {
                let mut characters = text.chars();
                if let (Some(c), None) = (characters.next(), characters.next()) {
                    found = Some((c, length));
                    break;
                }
            }
        }
        let Some(value) = found else { return 0 };
        value
    };
    if character.is_alphabetic()
        || unicode_normalization::char::is_combining_mark(character)
        || (!first && character.is_numeric())
    {
        length
    } else {
        0
    }
}
fn tokenize(s: &[u8], charset: &str) -> Vec<Token> {
    let mut ts = Vec::new();
    let mut i = 0;
    while i < s.len() {
        if s[i] == b' ' {
            i += 1;
            continue;
        }
        let start = i;
        let c = s[i];
        i += 1;
        let kind = if name_character(s, start, charset, true) > 0 {
            i = start + name_character(s, start, charset, true);
            while i < s.len() {
                let length = name_character(s, i, charset, false);
                if length == 0 {
                    break;
                }
                i += length;
            }
            Kind::Name
        } else if c.is_ascii_digit() {
            while i < s.len() && s[i].is_ascii_digit() {
                i += 1;
            }
            Kind::Number
        } else if matches!(c, b'\'' | b'"') {
            while i < s.len() && s[i] != c {
                i += 1;
            }
            if i < s.len() {
                i += 1;
            }
            Kind::String
        } else {
            Kind::Symbol(c)
        };
        ts.push(Token {
            kind,
            start,
            end: i,
        });
    }
    ts.push(Token {
        kind: Kind::End,
        start: s.len(),
        end: s.len(),
    });
    ts
}
pub(super) fn syntax(source: &[u8], offset: usize) -> QueryError {
    let bytes = &source[offset.min(source.len())..source.len().min(offset.saturating_add(32))];
    let mut primary = String::new();
    let mut condition = String::new();
    let mut at = 0;
    while at < bytes.len() {
        let byte = bytes[at];
        let length = if byte < 128 {
            1
        } else if byte & 0xe0 == 0xc0 {
            2
        } else if byte & 0xf0 == 0xe0 {
            3
        } else if byte & 0xf8 == 0xf0 {
            4
        } else {
            1
        };
        let valid = bytes
            .get(at..at + length)
            .and_then(|s| std::str::from_utf8(s).ok());
        if length <= 3 {
            if let Some(text) = valid {
                primary.push_str(text);
                condition.push_str(text);
                at += length;
                continue;
            }
        }
        let count = if valid.is_some() { length } else { 1 };
        for &b in &bytes[at..at + count] {
            primary.push(char::from(b));
            condition.push('?');
        }
        at += count;
    }
    let message = format!("XPATH syntax error: '{primary}'");
    let error = crate::error::catalog::ER_UNKNOWN_ERROR.error(message.clone());
    if primary == condition {
        error
    } else {
        error.with_condition_information(std::collections::BTreeMap::from([(
            "MESSAGE_TEXT",
            format!("XPATH syntax error: '{condition}'"),
        )]))
    }
}
pub(super) fn compile(source: &[u8], charset: &'static str) -> Result<Program, QueryError> {
    let mut p = Parser {
        source,
        charset,
        tokens: tokenize(source, charset),
        at: 0,
        depth: 0,
        in_predicate: 0,
    };
    let e = p.expression(0)?;
    if p.token().kind != Kind::End {
        return Err(p.error());
    }
    let mut variables = Vec::new();
    let mut work_vectors = 1;
    let mut pending = vec![&e];
    while let Some(node) = pending.pop() {
        if let XExpr::Variable { name, user, offset } = node {
            variables.push((name.clone(), *user, *offset));
        }
        if node.nodeset() {
            work_vectors += 2;
        }
        node.children(&mut pending);
    }
    variables.sort_unstable_by_key(|v| v.2);
    Ok(Program {
        expression: e,
        variables,
        work_vectors,
    })
}
impl Parser<'_> {
    fn token(&self) -> Token {
        self.tokens[self.at]
    }
    fn adjacent_next(&self) -> bool {
        self.tokens
            .get(self.at + 1)
            .is_some_and(|next| self.token().end == next.start)
    }
    fn next_kind(&self) -> Kind {
        self.tokens.get(self.at + 1).map_or(Kind::End, |t| t.kind)
    }
    fn text(&self, t: Token) -> &[u8] {
        &self.source[t.start..t.end]
    }
    fn error(&self) -> QueryError {
        syntax(self.source, self.token().start)
    }
    fn eat(&mut self, c: u8) -> bool {
        if self.token().kind == Kind::Symbol(c) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, c: u8) -> Result<(), QueryError> {
        if self.eat(c) {
            Ok(())
        } else {
            Err(self.error())
        }
    }
    fn name(&mut self) -> Result<Vec<u8>, QueryError> {
        if self.token().kind != Kind::Name {
            return Err(self.error());
        }
        let word = String::from_utf8_lossy(self.text(self.token())).to_ascii_lowercase();
        if self.next_kind() == Kind::Symbol(b'(')
            && self.adjacent_next()
            && matches!(
                word.as_str(),
                "count"
                    | "sum"
                    | "not"
                    | "last"
                    | "true"
                    | "false"
                    | "floor"
                    | "round"
                    | "concat"
                    | "number"
                    | "boolean"
                    | "ceiling"
                    | "position"
                    | "contains"
                    | "substring"
                    | "string-length"
            )
        {
            return Err(self.error());
        }
        let mut n = self.text(self.token()).to_vec();
        self.at += 1;
        if self.token().kind == Kind::Symbol(b':') {
            self.at += 1;
            n.push(b':');
            if self.token().kind != Kind::Name {
                return Err(self.error());
            }
            n.extend_from_slice(self.text(self.token()));
            self.at += 1;
        }
        Ok(n)
    }
    fn operator(&self) -> Option<(u8, BinaryOp, usize)> {
        use BinaryOp::*;
        let t = self.token();
        if t.kind == Kind::Name {
            if self.next_kind() == Kind::Symbol(b'(') && self.adjacent_next() {
                return None;
            }
            let n = self.text(t);
            if n.eq_ignore_ascii_case(b"or") {
                return Some((1, Or, 1));
            }
            if n.eq_ignore_ascii_case(b"and") {
                return Some((2, And, 1));
            }
            if n.eq_ignore_ascii_case(b"div") {
                return Some((6, IntDiv, 1));
            }
            if n.eq_ignore_ascii_case(b"mod") {
                return Some((6, Mod, 1));
            }
        }
        match t.kind {
            Kind::Symbol(b'=') => Some((3, Eq, 1)),
            Kind::Symbol(b'!') if self.next_kind() == Kind::Symbol(b'=') => Some((3, NotEq, 2)),
            Kind::Symbol(b'<') => Some((
                4,
                if self.next_kind() == Kind::Symbol(b'=') {
                    LtEq
                } else {
                    Lt
                },
                if self.next_kind() == Kind::Symbol(b'=') {
                    2
                } else {
                    1
                },
            )),
            Kind::Symbol(b'>') => Some((
                4,
                if self.next_kind() == Kind::Symbol(b'=') {
                    GtEq
                } else {
                    Gt
                },
                if self.next_kind() == Kind::Symbol(b'=') {
                    2
                } else {
                    1
                },
            )),
            Kind::Symbol(b'+') => Some((5, Add, 1)),
            Kind::Symbol(b'-') => Some((5, Sub, 1)),
            Kind::Symbol(b'*') => Some((6, Mul, 1)),
            _ => None,
        }
    }
    fn expression(&mut self, min: u8) -> Result<XExpr, QueryError> {
        self.depth += 1;
        if self.depth > 256 {
            return Err(self.error());
        }
        let mut e = self.unary()?;
        while let Some((prec, op, n)) = self.operator() {
            if prec < min {
                break;
            }
            let offset = self.token().start;
            self.at += n;
            let right = self.expression(prec + 1)?;
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
            ) && e.nodeset()
                && right.nodeset()
            {
                let tail = &self.source[offset..self.source.len().min(offset + 32)];
                return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                    .error(format!(
                        "XPATH error: comparison of two nodesets is not supported: '{}'",
                        String::from_utf8_lossy(tail)
                    ))
                    .with_followup_error(self.error()));
            }
            let decimals = if matches!(
                op,
                BinaryOp::IntDiv
                    | BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
                    | BinaryOp::And
                    | BinaryOp::Or
            ) {
                Some(0)
            } else {
                e.decimals().zip(right.decimals()).map(|(a, b)| a.max(b))
            };
            e = XExpr::Binary {
                op,
                left: Box::new(e),
                right: Box::new(right),
                decimals,
            };
        }
        self.depth -= 1;
        Ok(e)
    }
    fn unary(&mut self) -> Result<XExpr, QueryError> {
        if self.eat(b'-') {
            self.depth += 1;
            if self.depth > 256 {
                return Err(self.error());
            }
            let e = self.unary()?;
            self.depth -= 1;
            return Ok(XExpr::Neg(Box::new(e)));
        }
        let mut e = self.primary_path()?;
        while self.eat(b'|') {
            if !e.nodeset() {
                return Err(self.error());
            }
            let right = self.primary_path()?;
            if !right.nodeset() {
                return Err(self.error());
            }
            e = XExpr::Union(Box::new(e), Box::new(right));
        }
        Ok(e)
    }
    fn primary_path(&mut self) -> Result<XExpr, QueryError> {
        let t = self.token();
        let location = matches!(t.kind, Kind::Symbol(b'/' | b'@' | b'.' | b'*'))
            || t.kind == Kind::Name
                && (self.next_kind() != Kind::Symbol(b'(')
                    || !self.adjacent_next()
                    || self.node_type(t));
        let mut e = if location {
            self.location(None)?
        } else {
            match t.kind {
                Kind::String => {
                    self.at += 1;
                    if t.end - t.start < 2 || self.source[t.end - 1] != self.source[t.start] {
                        return Err(syntax(self.source, t.start));
                    }
                    let value = super::decode(&self.source[t.start + 1..t.end - 1], self.charset)?;
                    XExpr::Literal(match value {
                        Value::String(s) if s.len() <= 31 => Value::SmallString(
                            crate::types::SmallStringBuf::from_slice(s.as_bytes()),
                        ),
                        v => v,
                    })
                }
                Kind::Number => {
                    self.at += 1;
                    let mut end = t.end;
                    if self.eat(b'.') {
                        end = self.tokens[self.at - 1].end;
                        if self.token().kind == Kind::Number {
                            end = self.token().end;
                            self.at += 1;
                        }
                    }
                    let number = String::from_utf8_lossy(&self.source[t.start..end]);
                    if let Some((_, fraction)) = number.split_once('.') {
                        let prefix = number.split(' ').next().unwrap_or(&number);
                        let value = prefix.parse::<f64>().unwrap_or(f64::INFINITY);
                        if !value.is_finite() {
                            let display = &number[..number.len().min(192)];
                            let error = crate::error::catalog::ER_ILLEGAL_VALUE_FOR_TYPE.error(
                                format!("Illegal double '{display}' value found during parsing"),
                            );
                            return Err(if self.token().kind == Kind::End {
                                error
                            } else {
                                error.with_followup_error(self.error())
                            });
                        }
                        XExpr::Real(
                            value,
                            fraction
                                .bytes()
                                .take_while(u8::is_ascii_digit)
                                .count()
                                .min(31) as u8,
                        )
                    } else {
                        let integer = number.parse::<u64>().unwrap_or(u64::MAX) as i64;
                        XExpr::Literal(Value::Int(integer))
                    }
                }
                Kind::Symbol(b'(') => {
                    self.at += 1;
                    let e = self.expression(0)?;
                    self.expect(b')')?;
                    e
                }
                Kind::Symbol(b'$') => {
                    self.at += 1;
                    let user = self.eat(b'@');
                    let name = self.name()?;
                    let name = String::from_utf8_lossy(&name).into_owned();
                    if !user && super::local_variable(&name).is_none() {
                        let tail = &self.source[t.start..self.source.len().min(t.start + 32)];
                        return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                            .error(format!(
                                "Unknown XPATH variable at: '{}'",
                                String::from_utf8_lossy(tail)
                            ))
                            .with_followup_error(self.error()));
                    }
                    XExpr::Variable {
                        name,
                        user,
                        offset: t.start,
                    }
                }
                Kind::Name => self.call()?,
                _ => return Err(self.error()),
            }
        };
        if self.token().kind == Kind::Symbol(b'/') {
            if !e.nodeset() {
                return Err(self.error());
            }
            e = self.location(Some(Box::new(e)))?;
        }
        Ok(e)
    }
    fn node_type(&self, t: Token) -> bool {
        [
            b"text".as_slice(),
            b"node",
            b"comment",
            b"processing-instruction",
        ]
        .iter()
        .any(|x| self.text(t).eq_ignore_ascii_case(x))
    }
    fn location(&mut self, base: Option<Box<XExpr>>) -> Result<XExpr, QueryError> {
        let mut root = false;
        let mut steps = Vec::new();
        if self.eat(b'/') {
            root = base.is_none();
            if self.eat(b'/') {
                steps.push(Step {
                    axis: Axis::DescendantSelf,
                    name: b"*".to_vec(),
                    predicates: Vec::new(),
                });
            } else if matches!(
                self.token().kind,
                Kind::End
                    | Kind::Symbol(b')' | b']' | b',' | b'|' | b'=' | b'<' | b'>' | b'+' | b'-')
            ) {
                return Ok(XExpr::Path { base, root, steps });
            }
        }
        loop {
            let mut axis = Axis::Child;
            let mut name = b"*".to_vec();
            if self.eat(b'.') {
                axis = if self.eat(b'.') {
                    Axis::Parent
                } else {
                    Axis::Identity
                };
                if self.token().kind == Kind::Symbol(b'[') {
                    return Err(self.error());
                }
            } else {
                if self.eat(b'@') {
                    axis = Axis::Attribute;
                } else if self.token().kind == Kind::Name
                    && self.adjacent_next()
                    && self.next_kind() == Kind::Symbol(b':')
                    && self.tokens.get(self.at + 2).is_some_and(|t| {
                        t.kind == Kind::Symbol(b':') && t.start == self.tokens[self.at + 1].end
                    })
                {
                    let a = String::from_utf8_lossy(self.text(self.token())).to_ascii_lowercase();
                    if !matches!(
                        a.as_str(),
                        "child"
                            | "attribute"
                            | "self"
                            | "parent"
                            | "ancestor"
                            | "ancestor-or-self"
                            | "descendant"
                            | "descendant-or-self"
                            | "following"
                            | "following-sibling"
                            | "preceding"
                            | "preceding-sibling"
                            | "namespace"
                    ) {
                        self.at += 2;
                        return Err(self.error());
                    }
                    self.at += 3;
                    axis = match a.as_str() {
                        "child" => Axis::Child,
                        "attribute" => Axis::Attribute,
                        "self" => Axis::SelfNode,
                        "parent" => Axis::Parent,
                        "ancestor" => Axis::Ancestor,
                        "ancestor-or-self" => Axis::AncestorSelf,
                        "descendant" => Axis::Descendant,
                        "descendant-or-self" => Axis::DescendantSelf,
                        "following" | "following-sibling" | "preceding" | "preceding-sibling"
                        | "namespace" => Axis::Child,
                        _ => return Err(self.error()),
                    };
                }
                if self.token().kind == Kind::Name
                    && self.next_kind() == Kind::Symbol(b'(')
                    && self.adjacent_next()
                    && self.node_type(self.token())
                {
                    self.at += 1;
                    self.expect(b'(')?;
                    self.expect(b')')?;
                    axis = Axis::Identity;
                } else if !self.eat(b'*') {
                    name = self.name()?;
                }
            }
            let mut predicates = Vec::new();
            while self.eat(b'[') {
                self.in_predicate += 1;
                predicates.push(self.expression(0)?);
                self.in_predicate -= 1;
                self.expect(b']')?;
            }
            steps.push(Step {
                axis,
                name,
                predicates,
            });
            if !self.eat(b'/') {
                break;
            }
            if self.eat(b'/') {
                steps.push(Step {
                    axis: Axis::DescendantSelf,
                    name: b"*".to_vec(),
                    predicates: Vec::new(),
                });
            }
        }
        Ok(XExpr::Path { base, root, steps })
    }
    fn call(&mut self) -> Result<XExpr, QueryError> {
        let token = self.token();
        let name = String::from_utf8_lossy(self.text(token)).to_ascii_lowercase();
        let (min, max) = match name.as_str() {
            "true" | "false" | "position" | "last" => (0, 0),
            "number" | "string-length" => (0, 1),
            "sum" | "count" | "not" | "boolean" | "ceiling" | "floor" | "round" => (1, 1),
            "contains" => (2, 2),
            "substring" => (2, 3),
            "concat" => (2, 255),
            _ => {
                self.at += 1;
                return Err(self.error());
            }
        };
        self.at += 1;
        self.expect(b'(')?;
        let mut args = Vec::new();
        if !self.eat(b')') {
            loop {
                args.push(self.expression(0)?);
                if self.eat(b')') {
                    break;
                }
                self.expect(b',')?;
                if args.len() >= max {
                    return Err(self.error());
                }
            }
        }
        if args.len() < min || args.len() > max {
            return Err(self.error());
        }
        if matches!(name.as_str(), "count" | "sum") && !args[0].nodeset() {
            return Err(self.error());
        }
        if self.in_predicate == 0
            && (matches!(name.as_str(), "position" | "last")
                || name == "string-length" && args.is_empty())
        {
            return Err(self.error());
        }
        Ok(XExpr::Call { name, args })
    }
}
