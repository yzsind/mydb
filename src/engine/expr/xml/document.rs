//! Offset-based XML fragment reader. Text and entity spelling remain untouched.
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::types::QueryError;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Kind {
    Element,
    Attribute,
    Text,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct Node {
    pub kind: Kind,
    pub parent: usize,
    pub end: usize,
    pub name: (usize, usize),
    pub span: (usize, usize),
}
#[derive(Default)]
pub(super) struct Document {
    pub nodes: Vec<Node>,
    charge: Option<ControlledMemoryCharge>,
}
enum ParseError {
    Syntax(String),
    Query(QueryError),
}
impl From<QueryError> for ParseError {
    fn from(e: QueryError) -> Self {
        Self::Query(e)
    }
}
impl From<&str> for ParseError {
    fn from(e: &str) -> Self {
        Self::Syntax(e.to_owned())
    }
}
impl From<String> for ParseError {
    fn from(e: String) -> Self {
        Self::Syntax(e)
    }
}

#[derive(Clone, Copy)]
struct Token {
    kind: u8,
    start: usize,
    end: usize,
}
fn space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r')
}
fn name_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c >= 128 || matches!(c, b'_' | b':')
}
fn name_part(c: u8) -> bool {
    name_start(c) || c.is_ascii_digit() || matches!(c, b'-' | b'.')
}
fn scan(s: &[u8], p: &mut usize) -> Token {
    while *p < s.len() && space(s[*p]) {
        *p += 1;
    }
    let start = *p;
    if start == s.len() {
        return Token {
            kind: b'E',
            start,
            end: start,
        };
    }
    let c = s[*p];
    if s[start..].starts_with(b"<!--") {
        *p = memchr::memmem::find(&s[start + 4..], b"-->").map_or(s.len(), |n| start + 4 + n + 3);
        return Token {
            kind: b'C',
            start,
            end: *p,
        };
    }
    if matches!(c, b'\'' | b'"') {
        *p += 1;
        let start = *p;
        *p = memchr::memchr(c, &s[*p..]).map_or(s.len(), |n| *p + n);
        let end = *p;
        if *p < s.len() {
            *p += 1;
        }
        return Token {
            kind: b'S',
            start,
            end,
        };
    }
    if name_start(c) {
        *p += 1;
        while *p < s.len() && name_part(s[*p]) {
            *p += 1;
        }
        return Token {
            kind: b'I',
            start,
            end: *p,
        };
    }
    if b"?=/<>!".contains(&c) {
        *p += 1;
        return Token {
            kind: c,
            start,
            end: *p,
        };
    }
    Token {
        kind: b'U',
        start,
        end: start,
    }
}
fn token_name(t: Token) -> &'static str {
    match t.kind {
        b'E' => "END-OF-INPUT",
        b'S' => "STRING",
        b'I' => "IDENT",
        b'=' => "'='",
        b'<' => "'<'",
        b'>' => "'>'",
        b'/' => "'/'",
        b'?' => "'?'",
        b'!' => "'!'",
        b'C' => "COMMENT",
        _ => "unknown token",
    }
}
fn unexpected(t: Token, wanted: &str) -> String {
    format!("{} unexpected ({wanted} wanted)", token_name(t))
}
impl Document {
    fn push(
        &mut self,
        kind: Kind,
        parent: usize,
        name: (usize, usize),
        span: (usize, usize),
    ) -> Result<usize, QueryError> {
        if self.nodes.len() == self.nodes.capacity() {
            let capacity = (self.nodes.capacity().max(32) * 2).max(self.nodes.len() + 1);
            let charge = ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::Executor,
                capacity.saturating_mul(std::mem::size_of::<Node>()),
            )?;
            self.nodes.reserve_exact(capacity - self.nodes.len());
            self.charge = Some(charge);
        }
        let id = self.nodes.len();
        self.nodes.push(Node {
            kind,
            parent,
            end: id + 1,
            name,
            span,
        });
        Ok(id)
    }
    pub fn parse(&mut self, s: &[u8]) -> Result<bool, QueryError> {
        self.nodes.clear();
        self.push(Kind::Element, 0, (0, 0), (0, s.len()))?;
        let mut p = 0;
        let mut stack = smallvec::SmallVec::<[usize; 32]>::new();
        stack.push(0);
        let outcome: Result<(), ParseError> = (|| {
            let mut ticks = 0;
            while p < s.len() {
                ticks += 1;
                if ticks % 1024 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                let parent = *stack.last().unwrap();
                if s[p] != b'<' {
                    let start = p;
                    p = memchr::memchr(b'<', &s[p..]).map_or(s.len(), |n| p + n);
                    self.push(Kind::Text, parent, (0, 0), (start, p))?;
                    continue;
                }
                if s[p..].starts_with(b"<![CDATA[") {
                    let start = p + 9;
                    if let Some(n) = memchr::memmem::find(&s[start..], b"]]>") {
                        p = start + n + 3;
                        self.push(Kind::Text, parent, (0, 0), (start, p - 3))?;
                        continue;
                    }
                    p = s.len();
                    return Err("unexpected END-OF-INPUT".into());
                }
                let t = scan(s, &mut p);
                if t.kind == b'C' {
                    continue;
                }
                let mut t = scan(s, &mut p);
                if t.kind == b'/' {
                    t = scan(s, &mut p);
                    if t.kind != b'I' {
                        return Err(unexpected(t, "ident").into());
                    }
                    let id = *stack.last().unwrap();
                    let expected = self.nodes[id].name;
                    // The MySQL fragment reader checks closing-name length.
                    if id == 0 || t.end - t.start != expected.1 - expected.0 {
                        let got = String::from_utf8_lossy(&s[t.start..t.end]);
                        return Err(if id == 0 {
                            format!(
                                "'</{}>' unexpected (END-OF-INPUT wanted)",
                                truncate(&got, 31)
                            )
                        } else {
                            format!(
                                "'</{}>' unexpected ('</{}>' wanted)",
                                truncate(&got, 31),
                                truncate(
                                    &String::from_utf8_lossy(
                                        &s[expected.0 + usize::from(self.nodes[id].parent == 0)
                                            ..expected.1]
                                    ),
                                    31
                                )
                            )
                        }
                        .into());
                    }
                    stack.pop();
                    self.nodes[id].end = self.nodes.len();
                    self.nodes[id].span.1 = p + 1;
                    t = scan(s, &mut p);
                    if t.kind != b'>' {
                        return Err(unexpected(t, "'>'").into());
                    }
                    continue;
                }
                let question = t.kind == b'?';
                let exclam = t.kind == b'!';
                if question || exclam {
                    t = scan(s, &mut p);
                }
                if t.kind != b'I' {
                    return Err(unexpected(t, "ident or '/'").into());
                }
                if stack.len() >= 255 {
                    return Err(String::new().into());
                }
                let id = self.push(
                    Kind::Element,
                    parent,
                    (t.start, t.end),
                    (t.start.saturating_sub(1), 0),
                )?;
                stack.push(id);
                t = scan(s, &mut p);
                while t.kind == b'I' || (exclam && t.kind == b'S') {
                    let attr = t;
                    t = scan(s, &mut p);
                    if t.kind == b'=' {
                        t = scan(s, &mut p);
                        if !matches!(t.kind, b'I' | b'S') {
                            return Err(unexpected(t, "ident or string").into());
                        }
                        if stack.len() >= 255 {
                            return Err(String::new().into());
                        }
                        let a = self.push(
                            Kind::Attribute,
                            id,
                            (attr.start, attr.end),
                            (attr.start, p),
                        )?;
                        self.push(Kind::Text, a, (0, 0), (t.start, t.end))?;
                        self.nodes[a].end = self.nodes.len();
                        t = scan(s, &mut p);
                    } else if t.kind == b'I' {
                        let a = self.push(
                            Kind::Attribute,
                            id,
                            (attr.start, attr.end),
                            (attr.start, p),
                        )?;
                        self.nodes[a].end = self.nodes.len();
                    } else if !(exclam && t.kind == b'S') {
                        break;
                    }
                }
                let closed = t.kind == b'/' || question || exclam;
                let mut tag_end = p.saturating_add(1);
                if t.kind == b'/' {
                    tag_end = p.saturating_add(1);
                    t = scan(s, &mut p);
                }
                if question {
                    if t.kind != b'?' {
                        return Err(unexpected(t, "'?'").into());
                    }
                    tag_end = p.saturating_add(1);
                    t = scan(s, &mut p);
                }
                if closed {
                    stack.pop();
                    self.nodes[id].end = self.nodes.len();
                    self.nodes[id].span.1 = tag_end.min(s.len());
                }
                if t.kind != b'>' {
                    return Err(unexpected(t, "'>'").into());
                }
            }
            if stack.len() > 1 {
                return Err("unexpected END-OF-INPUT".into());
            }
            Ok(())
        })();
        self.nodes[0].end = self.nodes.len();
        match outcome {
            Ok(()) => Ok(true),
            Err(ParseError::Query(e)) => Err(e),
            Err(ParseError::Syntax(e)) => {
                let line = 1 + memchr::memchr_iter(b'\n', &s[..p.min(s.len())]).count();
                let pos = p - s[..p.min(s.len())]
                    .iter()
                    .rposition(|&c| c == b'\n')
                    .unwrap_or(0)
                    + 1;
                let msg = format!("parse error at line {line} pos {pos}: {e}");
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::ER_WRONG_VALUE,
                    format!("Incorrect XML value: '{}'", truncate(&msg, 126)),
                );
                Ok(false)
            }
        }
    }
    pub fn name_matches(&self, s: &[u8], id: usize, name: &[u8]) -> bool {
        name == b"*" || s[self.nodes[id].name.0..self.nodes[id].name.1] == *name
    }
    pub fn text(&self, s: &[u8], ids: impl Iterator<Item = usize>) -> Vec<u8> {
        let ids: smallvec::SmallVec<[usize; 8]> = ids.collect();
        if ids.is_empty() {
            return Vec::new();
        }
        if ids.len() == 1 {
            let id = ids[0];
            let mut out = Vec::new();
            let mut i = id + 1;
            while i < self.nodes[id].end {
                let n = &self.nodes[i];
                if n.kind == Kind::Text {
                    if !out.is_empty() {
                        out.push(b' ');
                    }
                    out.extend_from_slice(&s[n.span.0..n.span.1]);
                }
                i = n.end;
            }
            return out;
        }
        let mut selected = vec![false; self.nodes.len()];
        for id in ids {
            selected[id] = true;
        }
        let mut out = Vec::new();
        for node in self.nodes.iter().skip(1) {
            if node.kind == Kind::Text && selected[node.parent] {
                if !out.is_empty() {
                    out.push(b' ');
                }
                out.extend_from_slice(&s[node.span.0..node.span.1]);
            }
        }
        out
    }
}
fn truncate(s: &str, n: usize) -> &str {
    let mut end = n.min(s.len());
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}
