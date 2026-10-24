//! Function-call cost model.
//!
//! Native functions are classified by evaluation cost. Unknown names use
//! the conservative default multiplier; determinism lives in the compiled
//! function registry used by expression planning.

use super::consts::consts as cbo_consts;

/// Cost (in cost-units) of a single invocation of a built-in function.
/// Returned in absolute cost-units; callers multiply by the row count
/// Execution-mode decisions remain with expression planning.
pub fn function_call_cost(name: &str) -> f64 {
    let consts = cbo_consts();
    let multiplier = function_multiplier(name);
    consts.cpu_operator_cost() * multiplier
}

/// Whether the cost registry recognizes `name` as a native SQL
/// function. Stored-function resolution uses this as its first namespace
/// discriminator so an unqualified native call cannot be captured merely
/// because a same-named stored function exists in the current schema.
pub(crate) fn is_known_builtin_function(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    crate::engine::builtin_catalog::is_declared_builtin(&name.to_ascii_uppercase())
        || function_class(&lower) != FunctionClass::Unknown
        || crate::engine::expr_type::builtin_spec(&name.to_ascii_uppercase()).is_some()
        || crate::engine::geo::sql::is_geo_function(&name.to_ascii_uppercase())
        || crate::engine::vector::sql::is_vec_function(&name.to_ascii_uppercase())
}

use crate::engine::builtin_catalog::CostClass as FunctionClass;
crate::engine::builtin_catalog::declare_cost_class!();

/// Cost-unit multiplier per function class. Multiplied by
/// `cbo_cpu_operator_cost` to get the per-call cost.
fn function_multiplier(name: &str) -> f64 {
    match function_class(&name.to_ascii_lowercase()) {
        FunctionClass::IntegerArith | FunctionClass::CheapString => 1.0,
        FunctionClass::DateTimeImmutable | FunctionClass::DateTimeStable => 2.0,
        FunctionClass::SessionDependent => 1.0,
        FunctionClass::RegexOrComplex => 50.0,
        FunctionClass::CryptoHash => 20.0,
        FunctionClass::Volatile => 5.0,
        FunctionClass::Unknown => 3.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cheap_functions_have_low_cost() {
        // Sanity: arithmetic / cheap string functions multiply by 1.0.
        let abs = function_call_cost("abs");
        let upper = function_call_cost("UPPER");
        let now = function_call_cost("now");
        assert!(abs > 0.0);
        assert!((upper - abs).abs() < 1e-12);
        assert!(now > abs); // stable date/time has 2x multiplier
    }

    #[test]
    fn regex_costs_more_than_simple_compare() {
        let regex = function_call_cost("regexp_like");
        let plain = function_call_cost("upper");
        assert!(regex > plain * 10.0, "regex={regex} plain={plain}");
    }

    #[test]
    fn crypto_costs_more_than_regular() {
        let md5 = function_call_cost("md5");
        let length = function_call_cost("length");
        assert!(md5 > length * 5.0);
    }

    #[test]
    fn case_insensitive_matching() {
        // The public API lowercases the input internally. The
        // private `function_class` requires already-lowercased input.
        assert_eq!(function_call_cost("UPPER"), function_call_cost("upper"));
    }

    #[test]
    fn unknown_falls_back_to_default() {
        let unknown = function_call_cost("totally_made_up");
        let known = function_call_cost("abs");
        assert!(unknown > known);
    }
}
