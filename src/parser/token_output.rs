//! Borrow the original lexer tape while retaining the complete parser path.

use super::{ParseError, Statement};
use sqlparser::tokenizer::TokenWithSpan;
use std::cell::{Cell, RefCell};

type Renderer = fn(&[TokenWithSpan], &str, &str) -> String;

struct Request {
    address: usize,
    len: usize,
    context: String,
    renderer: Renderer,
    output: Option<String>,
}

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static REQUEST: RefCell<Option<Request>> = const { RefCell::new(None) };
}

struct Scope {
    previous: Option<Request>,
    enabled: bool,
}

impl Drop for Scope {
    fn drop(&mut self) {
        REQUEST.with(|slot| *slot.borrow_mut() = self.previous.take());
        ENABLED.with(|enabled| enabled.set(self.enabled));
    }
}

pub(crate) fn parse_with_token_output(
    sql: &str,
    context: String,
    renderer: Renderer,
) -> Result<(Vec<Statement>, Option<String>, String), ParseError> {
    // Quoting modes and MySQL literal normalization can change the parser's
    // tape. Those cases retain the independent canonical digest tokenizer.
    let eligible = sql.len() <= 16 * 1024
        && sql.is_ascii()
        && !sql
            .as_bytes()
            .windows(2)
            .any(|bytes| bytes.eq_ignore_ascii_case(b"0x"))
        && !sql
            .bytes()
            .any(|byte| matches!(byte, b'\'' | b'"' | b'`' | b'\\' | b'#'));
    if !eligible {
        return super::parse_statements(sql).map(|statements| (statements, None, context));
    }
    let scope = Scope {
        previous: REQUEST.with(|slot| {
            slot.replace(Some(Request {
                address: sql.as_ptr() as usize,
                len: sql.len(),
                context,
                renderer,
                output: None,
            }))
        }),
        enabled: ENABLED.with(|enabled| enabled.replace(true)),
    };
    let parsed = super::parse_statements(sql);
    let request = REQUEST
        .with(|slot| slot.borrow_mut().take())
        .expect("active token output scope");
    drop(scope);
    parsed.map(|statements| (statements, request.output, request.context))
}

/// Called before any parser token rewrite. Pointer/length equality proves
/// this is the complete unchanged source, rather than a rewritten fragment.
/// ParserMemoryGuard already reserves the lexer/rewriter/AST syntax envelopes;
/// rendering borrows the tape and drops its scratch before AST construction.
#[inline]
pub(super) fn observe(sql: &str, tokens: &[TokenWithSpan]) {
    if !ENABLED.with(Cell::get) {
        return;
    }
    let request = REQUEST.with(|slot| slot.borrow_mut().take());
    let Some(mut request) = request else { return };
    if request.address == sql.as_ptr() as usize
        && request.len == sql.len()
        && request.output.is_none()
    {
        request.output = Some((request.renderer)(tokens, sql, &request.context));
    }
    REQUEST.with(|slot| *slot.borrow_mut() = Some(request));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(tokens: &[TokenWithSpan], _: &str, context: &str) -> String {
        format!("{context}:{}", tokens.len())
    }

    #[test]
    fn unchanged_source_is_observed_but_rewrites_and_errors_keep_full_validation() {
        let (_, output, _) = parse_with_token_output("SELECT 1", "test".into(), render).unwrap();
        assert!(output.is_some());
        for text in [
            "SELECT 'quoted'",
            "SELECT `x`",
            "/* comment */ SELECT 1",
            "TABLE t",
        ] {
            let (_, output, _) = parse_with_token_output(text, String::new(), render).unwrap();
            assert!(output.is_none(), "{text}");
        }
        assert!(parse_with_token_output("SELECT (", String::new(), render).is_err());
        assert!(!ENABLED.with(Cell::get));
        assert!(REQUEST.with(|slot| slot.borrow().is_none()));
    }

    #[test]
    fn observer_scope_recovers_after_unwind_and_nested_parsing() {
        fn nested(_: &[TokenWithSpan], _: &str, _: &str) -> String {
            let (_, inner, _) =
                parse_with_token_output("SELECT 2", "inner".into(), render).unwrap();
            inner.unwrap()
        }
        let (_, output, _) = parse_with_token_output("SELECT 1", String::new(), nested).unwrap();
        assert!(output.unwrap().starts_with("inner:"));
        let result = std::panic::catch_unwind(|| {
            parse_with_token_output("SELECT 1", String::new(), |_, _, _| panic!("test unwind"))
        });
        assert!(result.is_err());
        assert!(!ENABLED.with(Cell::get));
        assert!(REQUEST.with(|slot| slot.borrow().is_none()));
    }
}
