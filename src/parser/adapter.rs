//! sqlparser integration points. Extension tokens keep source coordinates;
//! grammar and conversion do not depend on patched SQL presentation text.
mod operators;
#[cfg(test)]
pub(crate) use operators::max_set_operation_branches;
pub(super) use operators::{
    is_sounds_like, parse_high_prefix, parse_logical_chain, parse_sounds_like, parse_sql,
    SOUNDS_LIKE_OP,
};

mod json_value;
pub(super) use json_value::parse_prefix as parse_json_value_prefix;

mod casts;
pub(super) use casts::parse_prefix as parse_cast_prefix;
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
};

/// Build scalar syntax without rendering or reparsing its arguments.
pub(super) fn scalar_function(name: Ident, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: name.into(),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)))
                .collect(),
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

/// Mirrors the upstream column-option parse scope without process/thread
/// state. Drop restores the flag on normal returns, errors, and unwinding.
pub(super) struct ColumnOptionScope<'a> {
    active: &'a std::cell::Cell<bool>,
    previous: bool,
}
impl<'a> ColumnOptionScope<'a> {
    pub(super) fn enter(active: &'a std::cell::Cell<bool>) -> Self {
        Self {
            active,
            previous: active.replace(true),
        }
    }
}
impl Drop for ColumnOptionScope<'_> {
    fn drop(&mut self) {
        self.active.set(self.previous);
    }
}

mod scalar_clauses;
pub(super) use scalar_clauses::parse_prefix as parse_scalar_clause_prefix;

mod interval;
pub(super) use interval::parse_prefix as parse_interval_prefix;

mod patterns;
pub(super) use patterns::{parse_infix as parse_pattern_infix, DYNAMIC_LIKE};

mod parameters;
pub(super) use parameters::{display as display_sql, index as parameter_index};

mod trim;
pub(super) use trim::parse_prefix as parse_trim_prefix;

mod column_types;
pub(super) use column_types::{
    decode_suffix as decode_column_type_suffix, parse_suffix as parse_column_type_suffix,
    TypeSuffix,
};

mod literals;

mod locks;
