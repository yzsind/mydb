//! Rewrite syntax nodes so escapes, comments, and scoped flags keep their meaning.
use regex_syntax::ast::{self, Ast, ClassSet, ClassSetItem, Flag};

fn parse(pattern: &str) -> Ast {
    ast::parse::Parser::new()
        .parse(pattern)
        .expect("internal regex fragment")
}
fn property(kind: &ast::ClassAsciiKind) -> Option<&'static str> {
    use ast::ClassAsciiKind::*;
    Some(match kind {
        Alnum => r"\p{Alphabetic}\p{Nd}",
        Alpha => r"\p{Alphabetic}",
        Ascii => return None,
        Blank => r"\p{Zs}\t",
        Cntrl => r"\p{Cc}",
        Digit => r"\p{Nd}",
        Graph => r"\p{L}\p{M}\p{N}\p{P}\p{S}\p{Cf}\p{Co}",
        Lower => r"\p{Lowercase}",
        Print => r"\p{L}\p{M}\p{N}\p{P}\p{S}\p{Cf}\p{Co}\p{Zs}",
        Punct => r"\p{P}",
        Space => r"\p{White_Space}",
        Upper => r"\p{Uppercase}",
        Word => r"\w",
        Xdigit => r"0-9A-Fa-fＡ-Ｆａ-ｆ\p{Nd}",
    })
}
fn class_set(set: &mut ClassSet) {
    match set {
        ClassSet::Item(item) => class_item(item),
        ClassSet::BinaryOp(op) => {
            class_set(&mut op.lhs);
            class_set(&mut op.rhs);
        }
    }
}
fn class_item(item: &mut ClassSetItem) {
    match item {
        ClassSetItem::Ascii(class) => {
            if let Some(property) = property(&class.kind) {
                let replacement = parse(&format!(
                    "[{}{property}]",
                    if class.negated { "^" } else { "" }
                ));
                let Ast::ClassBracketed(class) = &replacement else {
                    unreachable!()
                };
                *item = ClassSetItem::Bracketed(class.clone());
            }
        }
        ClassSetItem::Bracketed(class) => class_set(&mut class.kind),
        ClassSetItem::Union(union) => {
            for item in &mut union.items {
                class_item(item)
            }
        }
        _ => {}
    }
}
fn rewrite(ast: &mut Ast, dotall: &mut bool, unix: bool, nfa: bool, anchors: &mut bool) {
    match ast {
        Ast::Flags(flags) => {
            if let Some(state) = flags.flags.flag_state(Flag::DotMatchesNewLine) {
                *dotall = state;
            }
        }
        Ast::Dot(_) => {
            *anchors = true;
            if !nfa {
                // The ordinary engine is used only after proving this input
                // has no line terminators. Keep its native dot optimizations.
                return;
            }
            *ast = if *dotall {
                parse(r"(?:\r\n|[^\r]|\r(?mR:^))")
            } else {
                parse(if unix {
                    r"[^\n]"
                } else {
                    r"[^\n\r\x0b\x0c\x{85}\x{2028}\x{2029}]"
                })
            };
        }
        Ast::Assertion(assertion) => {
            if matches!(
                assertion.kind,
                ast::AssertionKind::StartLine
                    | ast::AssertionKind::EndLine
                    | ast::AssertionKind::EndText
            ) {
                *anchors = true;
            }
            if nfa && assertion.kind == ast::AssertionKind::EndText {
                *ast = parse(r"(?mR:$)");
            }
        }
        Ast::ClassBracketed(class) => class_set(&mut class.kind),
        Ast::Repetition(rep) => rewrite(&mut rep.ast, dotall, unix, nfa, anchors),
        Ast::Group(group) => {
            let mut local = group
                .flags()
                .and_then(|f| f.flag_state(Flag::DotMatchesNewLine))
                .unwrap_or(*dotall);
            rewrite(&mut group.ast, &mut local, unix, nfa, anchors);
        }
        Ast::Concat(concat) => {
            for child in &mut concat.asts {
                rewrite(child, dotall, unix, nfa, anchors)
            }
        }
        Ast::Alternation(alt) => {
            for child in &mut alt.asts {
                rewrite(child, dotall, unix, nfa, anchors)
            }
        }
        _ => {}
    }
}
pub(crate) fn translate(pattern: &str, dotall: bool, unix: bool) -> (String, String, bool) {
    // These patterns need no syntax rewrite. Keep cold literal/anchor
    // compilation inexpensive; extra boundary programs are initialized lazily.
    if !pattern.contains('.') && !pattern.contains("[:") && !pattern.contains(r"\z") {
        return (
            pattern.to_owned(),
            pattern.to_owned(),
            pattern.contains(['^', '$']),
        );
    }
    let Ok(mut fast) = ast::parse::Parser::new().parse(pattern) else {
        // The ordinary compiler retains its detailed MySQL syntax diagnostic.
        return (pattern.to_owned(), pattern.to_owned(), false);
    };
    let mut nfa = fast.clone();
    let mut anchors = false;
    let mut fast_dotall = dotall;
    let mut nfa_dotall = dotall;
    rewrite(&mut fast, &mut fast_dotall, unix, false, &mut anchors);
    rewrite(&mut nfa, &mut nfa_dotall, unix, true, &mut anchors);
    (fast.to_string(), nfa.to_string(), anchors)
}
