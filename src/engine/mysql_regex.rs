//! Linear-time regex execution with SQL line-boundary semantics.
//!
//! Ordinary inputs use the optimized regex engine. Anchored searches on line
//! terminators use a prioritized Thompson VM with ICU-compatible boundaries.
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use regex_automata::{
    nfa::thompson::{State, NFA},
    util::{
        look::{Look, LookMatcher},
        primitives::StateID,
    },
};
use smallvec::SmallVec;
use std::sync::OnceLock;

/// Fuse the six candidate bytes into one vectorizable scan. Bitwise boolean
/// operations keep the chunk reduction free of per-byte branches.
#[inline]
fn possible_line(bytes: &[u8]) -> bool {
    let mut chunks = bytes.chunks_exact(64);
    for chunk in &mut chunks {
        let mask = chunk.iter().fold(0u8, |mask, &byte| {
            mask | u8::from((byte.wrapping_sub(10) < 4) | (byte == 0xc2) | (byte == 0xe2))
        });
        if mask != 0 {
            return true;
        }
    }
    chunks
        .remainder()
        .iter()
        .any(|&byte| (byte.wrapping_sub(10) < 4) | (byte == 0xc2) | (byte == 0xe2))
}

#[derive(Debug)]
pub(crate) struct Regex {
    fast: regex::Regex,
    lines: Option<LineProgram>,
    unix: bool,
}

impl Regex {
    pub(crate) fn compile(
        fast: regex::Regex,
        nfa_pattern: &str,
        insensitive: bool,
        multiline: bool,
        dotall: bool,
        unix: bool,
        anchors: bool,
    ) -> Result<Self, String> {
        let lines = anchors.then(|| LineProgram {
            pattern: nfa_pattern.to_owned(),
            insensitive,
            multiline,
            dotall,
            compiled: OnceLock::new(),
            initialize: parking_lot::Mutex::new(()),
        });
        Ok(Self { fast, lines, unix })
    }
    pub(crate) fn extra_bytes(&self) -> usize {
        self.lines
            .as_ref()
            .map_or(0, |lines| lines.pattern.capacity())
    }
    pub(crate) fn as_str(&self) -> &str {
        self.fast.as_str()
    }
    pub(crate) fn capture_names(&self) -> regex::CaptureNames<'_> {
        self.fast.capture_names()
    }
    pub(crate) fn captures_len(&self) -> usize {
        self.fast.captures_len()
    }
    pub(crate) fn line_free(&self, bytes: &[u8]) -> bool {
        self.lines.is_none()
            || !possible_line(bytes)
            || (memchr::memchr3(b'\n', b'\r', 0x0b, bytes).is_none()
                && (memchr::memchr3(0x0c, 0xc2, 0xe2, bytes).is_none()
                    || !memchr::memchr3_iter(0x0c, 0xc2, 0xe2, bytes).any(|at| {
                        bytes[at] == 0x0c
                            || bytes.get(at..at + 2) == Some(&[0xc2, 0x85])
                            || matches!(bytes.get(at..at + 3), Some([0xe2, 0x80, 0xa8 | 0xa9]))
                    })))
    }
    fn needs_lines(&self, s: &str) -> bool {
        !self.line_free(s.as_bytes())
    }
    pub(crate) fn is_match_line_free(&self, s: &str) -> bool {
        self.fast.is_match(s)
    }
    /// SQL value and batch kernels hold this reservation while searching.
    /// Admission failure is propagated, never converted into a non-match.
    pub(crate) fn search_scope(
        &self,
        s: &str,
    ) -> Result<
        Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
        crate::types::QueryError,
    > {
        self.search_scope_bytes(s.as_bytes())
    }
    pub(crate) fn search_scope_bytes(
        &self,
        bytes: &[u8],
    ) -> Result<
        Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
        crate::types::QueryError,
    > {
        if self.line_free(bytes) {
            return Ok(None);
        }
        let nfa = self.lines.as_ref().unwrap().get_or_compile()?;
        let slots = self.captures_len() * 2;
        let bytes = nfa.states().len().saturating_mul(
            slots
                .saturating_mul(std::mem::size_of::<Option<usize>>() * 4)
                .saturating_add(1024),
        );
        crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
            bytes,
        )
        .map(Some)
    }
    pub(crate) fn is_match(&self, s: &str) -> bool {
        if self.needs_lines(s) {
            self.slow_captures(s, 0).is_some()
        } else {
            self.fast.is_match(s)
        }
    }
    pub(crate) fn find<'a>(&self, s: &'a str) -> Option<Match<'a>> {
        self.find_at(s, 0)
    }
    fn find_at<'a>(&self, s: &'a str, at: usize) -> Option<Match<'a>> {
        if self.needs_lines(s) {
            self.captures_at(s, at)?.get(0)
        } else {
            self.fast.find_at(s, at).map(|m| Match {
                s,
                start: m.start(),
                end: m.end(),
            })
        }
    }
    pub(crate) fn captures_at<'a>(&self, s: &'a str, at: usize) -> Option<Captures<'a>> {
        if self.needs_lines(s) {
            Some(Captures::Slow {
                s,
                slots: self.slow_captures(s, at)?,
                names: self
                    .fast
                    .capture_names()
                    .map(|n| n.map(str::to_owned))
                    .collect(),
            })
        } else {
            self.fast
                .captures_at(s, at)
                .map(|c| Captures::Fast { c, s })
        }
    }
    pub(crate) fn find_iter<'r, 'a>(&'r self, s: &'a str) -> Matches<'r, 'a> {
        if self.needs_lines(s) {
            Matches::Slow {
                re: self,
                s,
                next: Some(0),
                previous_end: None,
            }
        } else {
            Matches::Fast {
                iter: self.fast.find_iter(s),
                s,
            }
        }
    }
    fn slow_captures(&self, s: &str, start: usize) -> Option<SmallVec<[Option<usize>; 8]>> {
        let nfa = &self
            .lines
            .as_ref()?
            .compiled
            .get()
            .expect("SQL search scope initializes the line program")
            .nfa;
        let states = nfa.states();
        let slots = self.captures_len() * 2;
        // A bounded number of states is visited at each byte, independent of
        // backtracking depth and the number of ways to reach a state.
        let mut roots: Vec<Thread> = Vec::new();
        let mut best = None;
        let mut seen = vec![false; states.len()];
        let mut ready = Vec::new();
        let mut stack = Vec::new();
        let mut at = start;
        loop {
            if best.is_none() && s.is_char_boundary(at) {
                roots.push(Thread {
                    id: nfa.start_anchored(),
                    slots: smallvec::smallvec![None;slots],
                });
            }
            seen.fill(false);
            ready.clear();
            for root in roots.drain(..) {
                stack.push(root);
                while let Some(mut thread) = stack.pop() {
                    if seen[thread.id.as_usize()] {
                        continue;
                    }
                    seen[thread.id.as_usize()] = true;
                    match &states[thread.id.as_usize()] {
                        State::Look { look, next } => {
                            if look_matches(*look, s, at, self.unix) {
                                thread.id = *next;
                                stack.push(thread);
                            }
                        }
                        State::Capture { slot, next, .. } => {
                            thread.slots[slot.as_usize()] = Some(at);
                            thread.id = *next;
                            stack.push(thread);
                        }
                        State::Union { alternates } => {
                            for &id in alternates.iter().rev() {
                                stack.push(Thread {
                                    id,
                                    slots: thread.slots.clone(),
                                });
                            }
                        }
                        State::BinaryUnion { alt1, alt2 } => {
                            stack.push(Thread {
                                id: *alt2,
                                slots: thread.slots.clone(),
                            });
                            thread.id = *alt1;
                            stack.push(thread);
                        }
                        State::Fail => {}
                        _ => ready.push(thread),
                    }
                }
            }
            for mut thread in ready.drain(..) {
                let next = match &states[thread.id.as_usize()] {
                    State::Match { .. } => {
                        best = Some(thread.slots);
                        break;
                    }
                    State::ByteRange { trans } => {
                        trans.matches(s.as_bytes(), at).then_some(trans.next)
                    }
                    State::Sparse(trans) => trans.matches(s.as_bytes(), at),
                    State::Dense(trans) => trans.matches(s.as_bytes(), at),
                    _ => unreachable!(),
                };
                if let Some(id) = next {
                    thread.id = id;
                    roots.push(thread);
                }
            }
            if at == s.len() || (best.is_some() && roots.is_empty()) {
                return best;
            }
            at += 1;
        }
    }
}
/// Compile the alternate boundary program only when an input needs it.
/// Ordinary varying patterns must not pay for a second NFA they never use.
#[derive(Debug)]
struct LineProgram {
    pattern: String,
    insensitive: bool,
    multiline: bool,
    dotall: bool,
    compiled: OnceLock<CompiledLines>,
    initialize: parking_lot::Mutex<()>,
}
#[derive(Debug)]
struct CompiledLines {
    nfa: NFA,
    _memory: ControlledMemoryCharge,
}
impl LineProgram {
    fn get_or_compile(&self) -> Result<&NFA, crate::types::QueryError> {
        if let Some(compiled) = self.compiled.get() {
            return Ok(&compiled.nfa);
        }
        let _guard = self.initialize.lock();
        if self.compiled.get().is_none() {
            // Reserve the compiler's explicit size ceiling before building,
            // then retain only the actual program size in the derived cache.
            let limit = 8 * 1024 * 1024;
            let mut memory =
                ControlledMemoryCharge::try_new(ControlledMemoryCategory::DerivedCache, limit)?;
            let mut compiler = NFA::compiler();
            compiler.configure(NFA::config().nfa_size_limit(Some(limit)));
            compiler.syntax(
                regex_automata::util::syntax::Config::new()
                    .case_insensitive(self.insensitive)
                    .multi_line(self.multiline)
                    .dot_matches_new_line(self.dotall),
            );
            let nfa = compiler.build(&self.pattern).map_err(|_| {
                crate::error::catalog::ER_REGEXP_INTERNAL_ERROR
                    .error("regular expression exceeded the implementation resource limit")
            })?;
            let actual = nfa.memory_usage();
            if actual > memory.bytes() {
                memory.try_grow(actual - memory.bytes())?;
            } else {
                memory.shrink(memory.bytes() - actual);
            }
            let _ = self.compiled.set(CompiledLines {
                nfa,
                _memory: memory,
            });
        }
        Ok(&self
            .compiled
            .get()
            .expect("initialized under compilation lock")
            .nfa)
    }
}
#[derive(Clone)]
struct Thread {
    id: StateID,
    slots: SmallVec<[Option<usize>; 8]>,
}
fn line_len(s: &str, at: usize, unix: bool) -> Option<usize> {
    if !s.is_char_boundary(at) {
        return None;
    }
    let tail = &s[at..];
    if tail.starts_with("\r\n") && !unix {
        return Some(2);
    }
    let c = tail.chars().next()?;
    (c == '\n'
        || (!unix
            && matches!(
                c,
                '\r' | '\u{b}' | '\u{c}' | '\u{85}' | '\u{2028}' | '\u{2029}'
            )))
    .then_some(c.len_utf8())
}
fn look_matches(look: Look, s: &str, at: usize, unix: bool) -> bool {
    match look {
        Look::Start => at == 0,
        // EndCRLF is reserved for an explicit \z, which always means EOF.
        Look::EndCRLF => at == s.len(),
        Look::End => {
            at == s.len()
                || line_len(s, at, unix).is_some_and(|n| {
                    at + n == s.len()
                        && !(s.as_bytes().get(at) == Some(&b'\n')
                            && at > 0
                            && s.as_bytes()[at - 1] == b'\r'
                            && !unix)
                })
        }
        Look::StartLF => {
            at == 0
                || ((unix || at < s.len())
                    && s.is_char_boundary(at)
                    && (1..=3).any(|n| {
                        at >= n
                            && line_len(s, at - n, unix) == Some(n)
                            && !(s.as_bytes()[at - 1] == b'\r'
                                && s.as_bytes().get(at) == Some(&b'\n')
                                && !unix)
                    }))
        }
        Look::EndLF => {
            at == s.len()
                || (line_len(s, at, unix).is_some()
                    && !(s.as_bytes().get(at) == Some(&b'\n')
                        && at > 0
                        && s.as_bytes()[at - 1] == b'\r'
                        && !unix))
        }
        Look::StartCRLF => s.as_bytes().get(at) != Some(&b'\n'),
        _ => LookMatcher::new().matches(look, s.as_bytes(), at),
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Match<'a> {
    s: &'a str,
    start: usize,
    end: usize,
}
impl<'a> Match<'a> {
    pub(crate) fn start(&self) -> usize {
        self.start
    }
    pub(crate) fn end(&self) -> usize {
        self.end
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.start == self.end
    }
    pub(crate) fn as_str(&self) -> &'a str {
        &self.s[self.start..self.end]
    }
}
pub(crate) enum Captures<'a> {
    Fast {
        c: regex::Captures<'a>,
        s: &'a str,
    },
    Slow {
        s: &'a str,
        slots: SmallVec<[Option<usize>; 8]>,
        names: Vec<Option<String>>,
    },
}
impl<'a> Captures<'a> {
    pub(crate) fn get(&self, i: usize) -> Option<Match<'a>> {
        match self {
            Self::Fast { c, s } => c.get(i).map(|m| Match {
                s,
                start: m.start(),
                end: m.end(),
            }),
            Self::Slow { s, slots, .. } => Some(Match {
                s,
                start: slots.get(i * 2).copied().flatten()?,
                end: slots.get(i * 2 + 1).copied().flatten()?,
            }),
        }
    }
    pub(crate) fn name(&self, name: &str) -> Option<Match<'a>> {
        match self {
            Self::Fast { c, s } => c.name(name).map(|m| Match {
                s,
                start: m.start(),
                end: m.end(),
            }),
            Self::Slow { names, .. } => {
                self.get(names.iter().position(|n| n.as_deref() == Some(name))?)
            }
        }
    }
}
pub(crate) enum Matches<'r, 'a> {
    Fast {
        iter: regex::Matches<'r, 'a>,
        s: &'a str,
    },
    Slow {
        re: &'r Regex,
        s: &'a str,
        next: Option<usize>,
        previous_end: Option<usize>,
    },
}
impl<'a> Iterator for Matches<'_, 'a> {
    type Item = Match<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Fast { iter, s } => iter.next().map(|m| Match {
                s,
                start: m.start(),
                end: m.end(),
            }),
            Self::Slow {
                re,
                s,
                next,
                previous_end,
            } => loop {
                let m = re.find_at(s, (*next)?)?;
                *next = if m.is_empty() {
                    if m.end == s.len() {
                        None
                    } else {
                        Some(m.end + s[m.end..].chars().next()?.len_utf8())
                    }
                } else {
                    Some(m.end)
                };
                if m.is_empty() && *previous_end == Some(m.end) {
                    continue;
                }
                *previous_end = Some(m.end);
                return Some(m);
            },
        }
    }
}

mod translate;
pub(crate) use translate::translate;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fused_line_detection_matches_unicode_boundaries_across_chunks() {
        let fast = regex::Regex::new(".").unwrap();
        let re = Regex::compile(fast, ".", false, false, false, false, true).unwrap();
        for padding in [0, 63] {
            for code in 0..=0xffff {
                let Some(character) = char::from_u32(code) else {
                    continue;
                };
                let mut text = "a".repeat(padding);
                text.push(character);
                text.push('a');
                let expected = !matches!(
                    character,
                    '\n' | '\r' | '\u{b}' | '\u{c}' | '\u{85}' | '\u{2028}' | '\u{2029}'
                );
                assert_eq!(
                    re.line_free(text.as_bytes()),
                    expected,
                    "{code:x}, padding={padding}"
                );
            }
        }
    }
    #[test]
    fn prioritized_line_vm_agrees_with_regex_on_lf_multiline_inputs() {
        for pattern in [
            "^a+$",
            "^(a+)(a*)$",
            "^a|b$",
            "^(a|ab)*$",
            "(?m:^)(a*?)(b?)$",
            "^.*?$",
            "^(?P<x>ab|a)+b?$",
            "^a?b*|a+$",
            "^\\b(a|b)+\\b$",
            "^a*$|b+",
        ] {
            let fast = regex::RegexBuilder::new(pattern)
                .multi_line(true)
                .build()
                .unwrap();
            let re =
                Regex::compile(fast.clone(), pattern, false, true, false, false, true).unwrap();
            let _scope = re.search_scope("a\n").unwrap();
            for number in 0..729 {
                let mut n = number;
                let mut s = String::new();
                for _ in 0..6 {
                    s.push(['a', 'b', '\n'][n % 3]);
                    n /= 3;
                }
                if s.ends_with('\n') {
                    continue;
                }
                for start in 0..=s.len() {
                    let expected = fast.captures_at(&s, start);
                    let actual = re.slow_captures(&s, start);
                    let expected = expected.map(|c| {
                        (0..c.len())
                            .flat_map(|i| [c.get(i).map(|m| m.start()), c.get(i).map(|m| m.end())])
                            .collect::<Vec<_>>()
                    });
                    assert_eq!(
                        actual.as_ref().map(|a| a.as_slice()),
                        expected.as_deref(),
                        "{pattern} {s:?} start={start}"
                    );
                }
            }
        }
    }
}
