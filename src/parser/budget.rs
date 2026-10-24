//! Shared resource boundaries for synchronous recursive parser work.
use super::ParseError;
use std::{cell::Cell, marker::PhantomData, rc::Rc};

/// MySQL's `MAX_SELECT_NESTING`: one nesting-map bit per query block level.
pub(crate) const MAX_SELECT_NESTING: usize = 63;

#[derive(Clone, Copy)]
pub(super) enum RecursionKind {
    Expression,
    Routine,
    Select,
}

thread_local! {
    static DEPTH: Cell<[usize; 3]> = const { Cell::new([0; 3]) };
}

/// A guard cannot cross threads: its counters and parser context are local.
pub(super) struct RecursionGuard {
    index: usize,
    _thread: PhantomData<Rc<()>>,
}

impl RecursionGuard {
    pub(super) fn enter(kind: RecursionKind) -> Result<Self, ParseError> {
        let index = kind as usize;
        DEPTH.with(|slot| {
            let mut depth = slot.get();
            if depth[index] >= Self::limit(kind) {
                return Err(Self::exceeded(kind));
            }
            depth[index] += 1;
            slot.set(depth);
            Ok(Self {
                index,
                _thread: PhantomData,
            })
        })
    }

    fn limit(kind: RecursionKind) -> usize {
        match kind {
            RecursionKind::Expression => crate::engine::expr_program::MAX_EXPRESSION_DEPTH,
            RecursionKind::Routine => 64,
            RecursionKind::Select => MAX_SELECT_NESTING,
        }
    }

    #[cold]
    fn exceeded(kind: RecursionKind) -> ParseError {
        let limit = Self::limit(kind);
        let (spec, message) = match kind {
            RecursionKind::Expression => (
                crate::error::catalog::ER_STACK_OVERRUN_NEED_MORE,
                format!("Expression nesting exceeds the maximum depth of {limit}"),
            ),
            RecursionKind::Routine => (
                crate::error::catalog::ER_PARSE_ERROR,
                format!("Routine nesting exceeds the maximum depth of {limit}"),
            ),
            RecursionKind::Select => (
                crate::error::catalog::ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT,
                "Too high level of nesting for select".to_string(),
            ),
        };
        let mut error = ParseError::at_zero(message);
        error.spec = spec;
        error
    }
}

impl Drop for RecursionGuard {
    fn drop(&mut self) {
        DEPTH.with(|slot| {
            let mut depth = slot.get();
            depth[self.index] -= 1;
            slot.set(depth);
        });
    }
}

pub(super) fn with_parser_stack<T>(f: impl FnOnce() -> T) -> Result<T, ParseError> {
    crate::util::stack::with_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
        f,
    )
    .map_err(|error| {
        let mut parsed = ParseError::at_zero(error.message());
        parsed.spec = error
            .spec()
            .unwrap_or(crate::error::catalog::ER_OUTOFMEMORY);
        parsed
    })
}

pub(super) const STACK_OOM: &str = "[mydb-parser-stack-1037] ";
pub(super) const STACK_INTERRUPTED: &str = "[mydb-parser-stack-1317] ";
pub(super) const STACK_TIMEOUT: &str = "[mydb-parser-stack-3024] ";
/// Token-level complexity rejection that maps to ER_STACK_OVERRUN_NEED_MORE.
pub(super) const STACK_DEPTH: &str = "[mydb-parser-stack-1436] ";

/// The dialect hook runs before upstream prefix parsing consumes a token.
/// Re-enter only that prefix on a charged stack segment; the caller retains
/// its recursion counter and the new segment sees the same parser/session TLS.
#[cold]
#[inline(never)]
pub(super) fn grow_sqlparser_prefix(
    parser: &mut sqlparser::parser::Parser<'_>,
) -> Result<sqlparser::ast::Expr, sqlparser::parser::ParserError> {
    crate::util::stack::with_reserved_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
        || parser.parse_prefix(),
    )
    .unwrap_or_else(|error| {
        let prefix = if error.is(crate::error::catalog::ER_QUERY_INTERRUPTED) {
            STACK_INTERRUPTED
        } else if error.is(crate::error::catalog::ER_QUERY_TIMEOUT) {
            STACK_TIMEOUT
        } else {
            STACK_OOM
        };
        Err(sqlparser::parser::ParserError::ParserError(format!(
            "{prefix}{}",
            error.message()
        )))
    })
}
