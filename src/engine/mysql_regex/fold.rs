//! Literal-run full folding for the prioritized Thompson VM.
//!
//! Character classes retain simple folding. A literal run consumes complete
//! original characters, so `ss` can match sharp-s while `s{2}` and `[s]` cannot
//! consume half of its expansion. Captures continue to index the original text.
use regex_syntax::ast::{self, Ast, Flag, GroupKind};

#[path = "fold_table.rs"]
mod table;

fn append(character: char, out: &mut String) {
    if let Some(text) = table::special(character) {
        out.push_str(text);
    } else {
        out.extend(character.to_lowercase());
    }
}

pub(super) fn may_expand(bytes: &[u8]) -> bool {
    bytes.iter().fold(0u8, |mask, &byte| {
        mask | u8::from(matches!(
            byte,
            0xc3 | 0xc4 | 0xc5 | 0xc7 | 0xce | 0xd6 | 0xe1 | 0xef
        ))
    }) != 0
}

pub(super) fn expanding(text: &str) -> bool {
    // Full-fold expansions begin with one of these UTF-8 lead bytes. Most
    // Unicode text (including CJK) needs no character-by-character folding
    // proof. The exhaustive scalar-value test keeps this filter conservative.
    may_expand(text.as_bytes())
        && text.chars().filter(|ch| !ch.is_ascii()).any(|ch| {
            table::special(ch).is_some_and(|fold| fold.chars().count() > 1)
                || ch.to_lowercase().count() > 1
        })
}

pub(super) fn pattern_expanding(pattern: &str) -> bool {
    if expanding(pattern) {
        return true;
    }
    if !pattern.contains("\\x") && !pattern.contains("\\u") {
        return false;
    }
    fn visit(ast: &Ast) -> bool {
        match ast {
            Ast::Literal(literal) => {
                let mut bytes = [0; 4];
                expanding(literal.c.encode_utf8(&mut bytes))
            }
            Ast::Group(group) => visit(&group.ast),
            Ast::Repetition(rep) => visit(&rep.ast),
            Ast::Concat(concat) => concat.asts.iter().any(visit),
            Ast::Alternation(alt) => alt.asts.iter().any(visit),
            _ => false,
        }
    }
    ast::parse::Parser::new()
        .parse(pattern)
        .is_ok_and(|ast| visit(&ast))
}

fn folded(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        append(ch, &mut out);
    }
    out
}

pub(super) fn match_at(subject: &str, at: usize, target: &str) -> Option<usize> {
    if !subject.is_char_boundary(at) {
        return None;
    }
    let mut remaining = target;
    for (offset, ch) in subject[at..].char_indices() {
        let mut buffer = smallvec::SmallVec::<[u8; 12]>::new();
        if let Some(text) = table::special(ch) {
            buffer.extend_from_slice(text.as_bytes());
        } else {
            for ch in ch.to_lowercase() {
                let mut bytes = [0; 4];
                buffer.extend_from_slice(ch.encode_utf8(&mut bytes).as_bytes());
            }
        }
        remaining = remaining.strip_prefix(std::str::from_utf8(&buffer).ok()?)?;
        if remaining.is_empty() {
            return Some(at + offset + ch.len_utf8());
        }
    }
    None
}

pub(super) struct Plan {
    pub pattern: String,
    pub captures: Vec<Option<usize>>,
    pub groups: Vec<Option<usize>>,
    pub literals: Vec<String>,
}

pub(super) fn prepare(pattern: &str, insensitive: bool) -> Option<Plan> {
    let mut ast = ast::parse::Parser::new().parse(pattern).ok()?;
    let mut prefix = "__mydb_full_fold_".to_owned();
    while pattern.contains(&prefix) {
        prefix.push('_');
    }
    let mut literals = Vec::new();
    mark(&mut ast, &mut { insensitive }, &prefix, &mut literals);
    let mut captures = vec![Some(0)];
    let mut groups = vec![None];
    collect_groups(&ast, &prefix, &mut captures, &mut groups);
    Some(Plan {
        pattern: ast.to_string(),
        captures,
        groups,
        literals,
    })
}

fn wrap(ast: &mut Ast, text: &str, prefix: &str, literals: &mut Vec<String>) {
    let name = format!("{prefix}{}", literals.len());
    literals.push(folded(text));
    let mut wrapper = ast::parse::Parser::new()
        .parse(&format!("(?P<{name}>x)"))
        .unwrap();
    let Ast::Group(group) = &mut wrapper else {
        unreachable!()
    };
    std::mem::swap(group.ast.as_mut(), ast);
    *ast = wrapper;
}

fn mark(ast: &mut Ast, insensitive: &mut bool, prefix: &str, literals: &mut Vec<String>) {
    match ast {
        Ast::Flags(flags) => {
            if let Some(state) = flags.flags.flag_state(Flag::CaseInsensitive) {
                *insensitive = state;
            }
        }
        Ast::Literal(literal) if *insensitive => {
            let text = literal.c.to_string();
            wrap(ast, &text, prefix, literals);
        }
        Ast::Concat(concat) => {
            let mut children = std::mem::take(&mut concat.asts).into_iter().peekable();
            while let Some(mut child) = children.next() {
                if *insensitive && matches!(child, Ast::Literal(_)) {
                    let mut run = vec![child];
                    while matches!(children.peek(), Some(Ast::Literal(_))) {
                        run.push(children.next().unwrap());
                    }
                    let text: String = run
                        .iter()
                        .filter_map(|node| match node {
                            Ast::Literal(lit) => Some(lit.c),
                            _ => None,
                        })
                        .collect();
                    child = if run.len() == 1 {
                        run.pop().unwrap()
                    } else {
                        let mut joined = concat.clone();
                        joined.asts = run;
                        Ast::Concat(joined)
                    };
                    wrap(&mut child, &text, prefix, literals);
                } else {
                    mark(&mut child, insensitive, prefix, literals);
                }
                concat.asts.push(child);
            }
        }
        Ast::Group(group) => {
            let mut local = group
                .flags()
                .and_then(|flags| flags.flag_state(Flag::CaseInsensitive))
                .unwrap_or(*insensitive);
            mark(&mut group.ast, &mut local, prefix, literals);
        }
        Ast::Repetition(rep) => mark(&mut rep.ast, insensitive, prefix, literals),
        Ast::Alternation(alt) => {
            // Unscoped flags continue through alternatives, matching the parser.
            for child in &mut alt.asts {
                mark(child, insensitive, prefix, literals);
            }
        }
        _ => {}
    }
}

fn collect_groups(
    ast: &Ast,
    prefix: &str,
    captures: &mut Vec<Option<usize>>,
    groups: &mut Vec<Option<usize>>,
) {
    match ast {
        Ast::Group(group) => {
            match &group.kind {
                GroupKind::CaptureIndex(index) => {
                    captures.push(Some(*index as usize));
                    groups.push(None);
                }
                GroupKind::CaptureName { name, .. } => {
                    if let Some(index) = name
                        .name
                        .strip_prefix(prefix)
                        .and_then(|value| value.parse().ok())
                    {
                        captures.push(None);
                        groups.push(Some(index));
                    } else {
                        captures.push(Some(name.index as usize));
                        groups.push(None);
                    }
                }
                GroupKind::NonCapturing(_) => {}
            }
            collect_groups(&group.ast, prefix, captures, groups);
        }
        Ast::Concat(concat) => {
            for child in &concat.asts {
                collect_groups(child, prefix, captures, groups);
            }
        }
        Ast::Alternation(alt) => {
            for child in &alt.asts {
                collect_groups(child, prefix, captures, groups);
            }
        }
        Ast::Repetition(rep) => collect_groups(&rep.ast, prefix, captures, groups),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn audit1106_expansion_filter_covers_all_unicode_scalars() {
        let mut folded = String::new();
        for character in (0..=0x10ffff).filter_map(char::from_u32) {
            folded.clear();
            append(character, &mut folded);
            let mut buffer = [0; 4];
            assert_eq!(
                expanding(character.encode_utf8(&mut buffer)),
                folded.chars().count() > 1,
                "U+{:04X}",
                character as u32
            );
        }
    }
}
