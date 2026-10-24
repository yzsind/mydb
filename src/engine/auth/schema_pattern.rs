//! Database grant patterns. Case folding follows the engine's identifier
//! policy; '%' and '_' operate on Unicode characters, not UTF-8 bytes.
use std::collections::{BTreeSet, HashSet, VecDeque};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Token {
    Literal(char),
    One,
    Many,
}
fn tokens(pattern: &str) -> Vec<Token> {
    let mut chars = pattern.chars();
    let mut out = Vec::new();
    while let Some(c) = chars.next() {
        out.push(match c {
            '\\' => Token::Literal(chars.next().unwrap_or('\\').to_ascii_lowercase()),
            '%' => Token::Many,
            '_' => Token::One,
            c => Token::Literal(c.to_ascii_lowercase()),
        });
    }
    out
}
pub(super) fn has_pattern(s: &str) -> bool {
    s.contains(['%', '_', '\\'])
}
pub(super) fn matches(pattern: &str, value: &str) -> bool {
    if !has_pattern(pattern) {
        return pattern.eq_ignore_ascii_case(value);
    }
    let p = tokens(pattern);
    let value: Vec<_> = value.chars().map(|c| c.to_ascii_lowercase()).collect();
    let (mut i, mut j, mut star, mut consumed) = (0, 0, None, 0);
    while j < value.len() {
        if p.get(i)
            .is_some_and(|t| *t == Token::One || *t == Token::Literal(value[j]))
        {
            i += 1;
            j += 1;
        } else if p.get(i) == Some(&Token::Many) {
            star = Some(i);
            i += 1;
            consumed = j;
        } else if let Some(k) = star {
            consumed += 1;
            j = consumed;
            i = k + 1;
        } else {
            return false;
        }
    }
    while p.get(i) == Some(&Token::Many) {
        i += 1;
    }
    i == p.len()
}
fn closure(p: &[Token], states: impl IntoIterator<Item = usize>) -> Vec<usize> {
    let mut out = BTreeSet::new();
    for mut i in states {
        out.insert(i);
        while p.get(i) == Some(&Token::Many) {
            i += 1;
            out.insert(i);
        }
    }
    out.into_iter().collect()
}
fn step(p: &[Token], states: &[usize], c: char) -> Vec<usize> {
    closure(
        p,
        states.iter().filter_map(|i| match p.get(*i) {
            Some(Token::Many) => Some(*i),
            Some(Token::One) => Some(i + 1),
            Some(Token::Literal(lit)) if *lit == c => Some(i + 1),
            _ => None,
        }),
    )
}
/// Language inclusion for delegation: matching a pattern string as if it
/// were a concrete database name can turn a one-character grant into '%'.
pub(super) fn covers(granted: &str, requested: &str) -> bool {
    if granted.eq_ignore_ascii_case(requested) {
        return true;
    }
    let g = tokens(granted);
    let r = tokens(requested);
    let mut alphabet = BTreeSet::new();
    for t in g.iter().chain(r.iter()) {
        if let Token::Literal(c) = t {
            alphabet.insert(*c);
        }
    }
    let other = (0..=char::MAX as u32)
        .filter_map(char::from_u32)
        .find(|c| !alphabet.contains(c))
        .unwrap();
    alphabet.insert(other);
    let start = (closure(&r, [0]), closure(&g, [0]));
    let mut seen = HashSet::from([start.clone()]);
    let mut queue = VecDeque::from([start]);
    while let Some((rs, gs)) = queue.pop_front() {
        if rs.contains(&r.len()) && !gs.contains(&g.len()) {
            return false;
        }
        for c in &alphabet {
            let next_r = step(&r, &rs, *c);
            if next_r.is_empty() {
                continue;
            }
            let pair = (next_r, step(&g, &gs, *c));
            if seen.insert(pair.clone()) {
                // Bound hostile pattern complexity; inability to prove
                // containment must never authorize a broader grant.
                if seen.len() > 4096 {
                    return false;
                }
                queue.push_back(pair);
            }
        }
    }
    true
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn delegation_checks_language_inclusion_not_literal_pattern_matching() {
        assert!(!covers("tenant_", "tenant%"));
        assert!(covers("tenant%", "tenant_a%"));
        assert!(covers("tenant\\_one", "tenant\\_one"));
        assert!(!covers("tenant\\_one", "tenant_one"));
        assert!(matches("tenant\\_%", "tenant_one"));
        assert!(!matches("tenant\\_%", "tenantXone"));
        assert!(matches("_", "猫"));
        assert!(!matches("é", "e"));
    }
}
