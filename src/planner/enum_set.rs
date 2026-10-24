//! ENUM/SET expression rewrites.

use super::*;

/// MySQL ENUM/SET columns sort and compare numerically — by declaration
/// ordinal (ENUM) / member bitmask (SET) — whenever the other operand
/// is a number, while the stored row value is the display string. The
/// planner rewrites the numeric contexts before physical planning:
///  * `ORDER BY enum_col` sorts by `__ENUM_ORDINAL(col, v1, …)`
///    (`__SET_ORDINAL` → bitmask for SET);
///  * `enum_col = / <> <integer literal>` with an in-range ordinal
///    swaps the literal for the matching value string, so index
///    lookups keep working;
///  * every other numeric comparison routes through the ordinal form
///    (NULL propagation comes free from the function's NULL handling).
pub(crate) fn rewrite_enum_set_numeric_semantics(db: &Database, select: &mut Select) {
    // A set-operation derived table's branches each run through
    // `prepare_select_for_planning` (which applies this rewrite) when executed
    // via the derived-table bridge, so only an ordinary-SELECT derived table
    // is rewritten here.
    if let Some(derived) = select.derived.as_mut().and_then(SubQuery::as_select_mut) {
        rewrite_enum_set_numeric_semantics(db, derived);
    }
    for join in &mut select.joins {
        if let Some(derived) = join.derived.as_mut().and_then(SubQuery::as_select_mut) {
            rewrite_enum_set_numeric_semantics(db, derived);
        }
    }
    let mut columns = EnumSetColumns::default();
    // `select.table` mirrors `from[0]` on single-table selects — dedupe
    // so the same table doesn't mark its own columns ambiguous.
    let mut seen: std::collections::HashSet<(String, Option<String>)> =
        std::collections::HashSet::new();
    let mut tables: Vec<(&str, Option<&str>)> = Vec::new();
    // `from` carries the binding's alias. The legacy `table` field is only
    // a fallback; registering both makes a single aliased source ambiguous.
    if select.from.is_empty() {
        if let Some(table) = select.table.as_deref() {
            if seen.insert((table.to_ascii_lowercase(), None)) {
                tables.push((table, None));
            }
        }
    }
    for table_ref in &select.from {
        let key = (
            table_ref.name.to_ascii_lowercase(),
            table_ref.alias.as_deref().map(str::to_ascii_lowercase),
        );
        if table_ref.alias.is_none() && seen.contains(&(table_ref.name.to_ascii_lowercase(), None))
        {
            continue;
        }
        if seen.insert(key) {
            tables.push((&table_ref.name, table_ref.alias.as_deref()));
        }
    }
    for join in &select.joins {
        let key = (
            join.table.name.to_ascii_lowercase(),
            join.table.alias.as_deref().map(str::to_ascii_lowercase),
        );
        if seen.insert(key) {
            tables.push((&join.table.name, join.table.alias.as_deref()));
        }
    }
    for (name, alias) in tables {
        columns.add_table(db, name, alias);
    }
    if columns.is_empty() {
        return;
    }
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            rewrite_enum_set_function_numeric_args(expr, &columns);
            rewrite_enum_set_comparisons(expr, &columns);
        }
    }
    for expression in &mut select.group_by {
        rewrite_enum_set_function_numeric_args(expression, &columns);
        rewrite_enum_set_comparisons(expression, &columns);
    }
    for order_by in &mut select.order_by {
        rewrite_enum_set_function_numeric_args(&mut order_by.expr, &columns);
        rewrite_enum_set_comparisons(&mut order_by.expr, &columns);
        if let Some(entry) = columns.resolve(&order_by.expr) {
            order_by.expr = ordinal_call(order_by.expr.clone(), entry);
        } else if let Some(entry) = enum_set_entry_in_string_expr(&order_by.expr, &columns) {
            if let Some(collation) = entry.collation.as_deref() {
                order_by.expr = Expr::Function {
                    name: "__COLLATION_SORT_KEY".to_string(),
                    args: vec![
                        order_by.expr.clone(),
                        Expr::Literal(crate::parser::Literal::String(collation.to_string())),
                    ],
                    distinct: false,
                };
            }
        }
    }
    if let Some(selection) = select.selection.as_mut() {
        rewrite_enum_set_function_numeric_args(selection, &columns);
        rewrite_enum_set_comparisons(selection, &columns);
    }
    if let Some(having) = select.having.as_mut() {
        rewrite_enum_set_function_numeric_args(having, &columns);
        rewrite_enum_set_comparisons(having, &columns);
    }
    for join in &mut select.joins {
        if let Some(condition) = join.condition.as_mut() {
            rewrite_enum_set_function_numeric_args(condition, &columns);
            rewrite_enum_set_comparisons(condition, &columns);
        }
    }
}

pub(super) fn rewrite_enum_set_function_numeric_args(expr: &mut Expr, columns: &EnumSetColumns) {
    match expr {
        Expr::Function { name, args, .. } => {
            if name.eq_ignore_ascii_case("CAST") && args.len() >= 2 {
                let numeric = matches!(&args[1], Expr::Literal(crate::parser::Literal::String(kind))
                    if matches!(kind.to_ascii_uppercase().as_str(), "SIGNED" | "UNSIGNED" | "DECIMAL" | "FLOAT" | "DOUBLE" | "REAL"));
                if numeric {
                    if let Some(entry) = columns.resolve(&args[0]) {
                        args[0] = ordinal_call(args[0].clone(), entry);
                    }
                }
            }
            if name.eq_ignore_ascii_case(crate::parser::GROUP_CONCAT_ORDERED_FN) {
                for order in args.iter_mut().skip(1).step_by(2) {
                    if let Some(entry) = columns.resolve(order) {
                        *order = ordinal_call(order.clone(), entry);
                    }
                }
            }
            if name.eq_ignore_ascii_case("FIND_IN_SET") && args.len() == 2 {
                if let Some(entry) = columns
                    .resolve(&args[1])
                    .filter(|entry| entry.is_set)
                    .cloned()
                {
                    let binary_collation = entry
                        .collation
                        .as_deref()
                        .and_then(|name| crate::charset::resolve_collation(name).ok())
                        .is_some_and(|collation| collation.binary);
                    *name = if binary_collation {
                        "__FIND_IN_SET_SET_BINARY".to_string()
                    } else {
                        "__FIND_IN_SET_SET".to_string()
                    };
                    args.extend(
                        entry.values.iter().map(|value| {
                            Expr::Literal(crate::parser::Literal::String(value.clone()))
                        }),
                    );
                }
            }
            if matches!(
                name.to_ascii_uppercase().as_str(),
                "ELT" | "MAKE_SET" | "EXPORT_SET"
            ) {
                if let Some(first) = args.first_mut() {
                    if let Some(entry) = columns.resolve(first).cloned() {
                        *first = ordinal_call(first.clone(), &entry);
                    }
                }
            }
            let numeric_args = matches!(
                name.to_ascii_uppercase().as_str(),
                "ABS"
                    | "SIGN"
                    | "CEIL"
                    | "CEILING"
                    | "FLOOR"
                    | "ROUND"
                    | "TRUNCATE"
                    | "MOD"
                    | "POW"
                    | "POWER"
                    | "SQRT"
                    | "EXP"
                    | "LN"
                    | "LOG"
                    | "LOG2"
                    | "LOG10"
                    | "SIN"
                    | "COS"
                    | "TAN"
                    | "ASIN"
                    | "ACOS"
                    | "ATAN"
                    | "ATAN2"
                    | "COT"
                    | "DEGREES"
                    | "RADIANS"
                    | "BIT_COUNT"
                    | "SUM"
                    | "AVG"
                    | "STD"
                    | "STDDEV"
                    | "STDDEV_POP"
                    | "STDDEV_SAMP"
                    | "VARIANCE"
                    | "VAR_POP"
                    | "VAR_SAMP"
                    | "BIT_AND"
                    | "BIT_OR"
                    | "BIT_XOR"
            );
            for arg in args {
                if numeric_args {
                    if let Some(entry) = columns.resolve(arg).cloned() {
                        *arg = ordinal_call(arg.clone(), &entry);
                    }
                }
                rewrite_enum_set_function_numeric_args(arg, columns);
            }
        }
        Expr::Unary { expr, op } => {
            if matches!(
                op,
                crate::parser::UnaryOp::Neg | crate::parser::UnaryOp::BitNot
            ) {
                if let Some(entry) = columns.resolve(expr).cloned() {
                    **expr = ordinal_call((**expr).clone(), &entry);
                }
            }
            rewrite_enum_set_function_numeric_args(expr, columns);
        }
        Expr::IsNull(expr) | Expr::IsNotNull(expr) | Expr::GroupKeyRef { expr, .. } => {
            rewrite_enum_set_function_numeric_args(expr, columns)
        }
        Expr::Binary { left, op, right } => {
            if matches!(
                op,
                BinaryOp::Add
                    | BinaryOp::Sub
                    | BinaryOp::Mul
                    | BinaryOp::Div
                    | BinaryOp::IntDiv
                    | BinaryOp::Mod
                    | BinaryOp::BitAnd
                    | BinaryOp::BitOr
                    | BinaryOp::BitXor
                    | BinaryOp::ShiftLeft
                    | BinaryOp::ShiftRight
            ) {
                if let Some(entry) = columns.resolve(left).cloned() {
                    **left = ordinal_call((**left).clone(), &entry);
                }
                if let Some(entry) = columns.resolve(right).cloned() {
                    **right = ordinal_call((**right).clone(), &entry);
                }
            }
            rewrite_enum_set_function_numeric_args(left, columns);
            rewrite_enum_set_function_numeric_args(right, columns);
        }
        Expr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            rewrite_enum_set_function_numeric_args(left, columns);
            rewrite_enum_set_function_numeric_args(right, columns);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_enum_set_function_numeric_args(expr, columns);
            rewrite_enum_set_function_numeric_args(low, columns);
            rewrite_enum_set_function_numeric_args(high, columns);
        }
        Expr::InList { expr, list, .. } => {
            rewrite_enum_set_function_numeric_args(expr, columns);
            for item in list {
                rewrite_enum_set_function_numeric_args(item, columns);
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                rewrite_enum_set_function_numeric_args(operand, columns);
            }
            for child in conditions.iter_mut().chain(results.iter_mut()) {
                rewrite_enum_set_function_numeric_args(child, columns);
            }
            if let Some(otherwise) = else_result {
                rewrite_enum_set_function_numeric_args(otherwise, columns);
            }
        }
        Expr::Tuple(items) => {
            for item in items {
                rewrite_enum_set_function_numeric_args(item, columns);
            }
        }
        _ => {}
    }
}

pub(super) fn enum_set_entry_in_string_expr<'a>(
    expr: &Expr,
    columns: &'a EnumSetColumns,
) -> Option<&'a EnumSetEntry> {
    match expr {
        Expr::Function { args, .. } if args.len() == 1 => columns.resolve(&args[0]),
        Expr::Unary { expr, .. } | Expr::GroupKeyRef { expr, .. } => columns.resolve(expr),
        _ => None,
    }
}

pub(super) fn rewrite_enum_set_comparisons(expr: &mut Expr, columns: &EnumSetColumns) {
    match expr {
        Expr::InList {
            expr: left,
            list,
            negated,
        } if columns.resolve(left).is_some() && !list.is_empty() => {
            let mut comparisons = list
                .iter()
                .map(|item| Expr::Binary {
                    left: left.clone(),
                    op: BinaryOp::Eq,
                    right: Box::new(item.clone()),
                })
                .collect::<Vec<_>>();
            for comparison in &mut comparisons {
                rewrite_enum_set_comparisons(comparison, columns);
            }
            let replacement = or_disjuncts(comparisons).unwrap();
            *expr = if *negated {
                Expr::Unary {
                    op: crate::parser::UnaryOp::Not,
                    expr: Box::new(replacement),
                }
            } else {
                replacement
            };
        }
        Expr::Between {
            expr: left,
            low,
            high,
            negated,
        } if columns.resolve(left).is_some() => {
            let mut replacement = Expr::Binary {
                left: Box::new(Expr::Binary {
                    left: left.clone(),
                    op: BinaryOp::GtEq,
                    right: low.clone(),
                }),
                op: BinaryOp::And,
                right: Box::new(Expr::Binary {
                    left: left.clone(),
                    op: BinaryOp::LtEq,
                    right: high.clone(),
                }),
            };
            rewrite_enum_set_comparisons(&mut replacement, columns);
            *expr = if *negated {
                Expr::Unary {
                    op: crate::parser::UnaryOp::Not,
                    expr: Box::new(replacement),
                }
            } else {
                replacement
            };
        }
        Expr::Binary { left, op, right } => {
            rewrite_enum_set_comparisons(left, columns);
            rewrite_enum_set_comparisons(right, columns);
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NullSafeEq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
            ) {
                if let Some(replacement) = enum_set_cmp_replacement(left, *op, right, columns) {
                    *expr = replacement;
                }
            }
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => rewrite_enum_set_comparisons(expr, columns),
        Expr::Function { args, .. } | Expr::Tuple(args) => {
            for e in args {
                rewrite_enum_set_comparisons(e, columns);
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if operand
                .as_deref()
                .is_some_and(|e| columns.resolve(e).is_some())
            {
                let compared = operand.take().unwrap();
                for condition in conditions.iter_mut() {
                    *condition = Expr::Binary {
                        left: compared.clone(),
                        op: BinaryOp::Eq,
                        right: Box::new(condition.clone()),
                    };
                }
            }
            for e in operand.iter_mut().chain(else_result.iter_mut()) {
                rewrite_enum_set_comparisons(e, columns);
            }
            for e in conditions.iter_mut().chain(results.iter_mut()) {
                rewrite_enum_set_comparisons(e, columns);
            }
        }
        Expr::InList { expr, list, .. } => {
            rewrite_enum_set_comparisons(expr, columns);
            for e in list {
                rewrite_enum_set_comparisons(e, columns);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            for e in [expr, low, high] {
                rewrite_enum_set_comparisons(e, columns);
            }
        }
        Expr::Like { expr, pattern, .. } => {
            for e in [expr, pattern] {
                rewrite_enum_set_comparisons(e, columns);
            }
        }
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                ..
            } = spec.as_mut();

            for e in args.iter_mut().chain(partition_by.iter_mut()) {
                rewrite_enum_set_comparisons(e, columns);
            }
            for o in order_by {
                rewrite_enum_set_comparisons(&mut o.expr, columns);
            }
        }
        _ => {}
    }
}

/// Build the replacement node for one `ENUM/SET <cmp> operand`
/// comparison, or `None` to leave it untouched.
///
/// * A **string literal** operand stays untouched: MySQL compares
///   ENUM/SET against string constants as plain strings, which the
///   default comparison already does (and index lookups keep working).
/// * An ENUM `= / <=> / <>` against a **visible in-range integer**
///   swaps the literal for the value string — same row set,
///   index-friendly.
/// * Every other numeric literal or **parameter** operand (auto-param
///   folds WHERE literals into placeholders before this pass; prepared
///   statements bind late) becomes a runtime-polymorphic `__ENUM_CMP`
///   / `__SET_CMP` call: the bound value's type decides between
///   ordinal/bitmask and string comparison at eval time — MySQL's
///   literal-type-driven rule, safe to share across parameter types in
///   the plan cache.
pub(super) fn enum_set_cmp_replacement(
    left: &mut Expr,
    op: BinaryOp,
    right: &mut Expr,
    columns: &EnumSetColumns,
) -> Option<Expr> {
    let (column_on_left, entry) = if let Some(entry) = columns.resolve(left) {
        if !enum_operand_shape(right, op) {
            return None;
        }
        (true, entry.clone())
    } else if let Some(entry) = columns.resolve(right) {
        if !enum_operand_shape(left, op) {
            return None;
        }
        (false, entry.clone())
    } else {
        return None;
    };
    let (column_expr, operand_expr) = if column_on_left {
        (&mut *left, &mut *right)
    } else {
        (&mut *right, &mut *left)
    };
    // ENUM equality against a visible in-range integer ordinal: swap
    // the literal for the value string — exact same row set, and index
    // lookups on the column still apply. 0 is the empty error value.
    if !entry.is_set && matches!(op, BinaryOp::Eq | BinaryOp::NullSafeEq | BinaryOp::NotEq) {
        if let Some(ordinal) = integer_literal(operand_expr) {
            let substituted = if ordinal == 0 {
                Some(String::new())
            } else if ordinal >= 1 && (ordinal as usize) <= entry.values.len() {
                Some(entry.values[ordinal as usize - 1].clone())
            } else {
                // Out-of-range ordinal falls through to the runtime
                // form: never matches, NULL propagation stays correct.
                None
            };
            if let Some(value) = substituted {
                *operand_expr = Expr::Literal(crate::parser::Literal::String(value));
                return None;
            }
        }
    }
    // Mirror the operator when the column sits on the right so the
    // runtime function always computes `column <op> operand`.
    let effective_op = if column_on_left {
        op
    } else {
        mirror_cmp_op(op)
    };
    let binary_collation = entry
        .collation
        .as_deref()
        .and_then(|name| crate::charset::resolve_collation(name).ok())
        .is_some_and(|collation| collation.binary);
    let name = if entry.is_set && binary_collation {
        "__SET_CMP_BINARY"
    } else if entry.is_set {
        "__SET_CMP"
    } else {
        "__ENUM_CMP"
    };
    let mut args = Vec::with_capacity(entry.values.len() + 3);
    args.push(column_expr.clone());
    args.push(operand_expr.clone());
    args.push(Expr::Literal(crate::parser::Literal::String(
        cmp_op_token(effective_op).to_string(),
    )));
    args.extend(
        entry
            .values
            .iter()
            .map(|v| Expr::Literal(crate::parser::Literal::String(v.clone()))),
    );
    Some(Expr::Function {
        name: name.to_string(),
        args,
        distinct: false,
    })
}

/// Operand shapes that route an ENUM/SET comparison through the runtime
/// `__ENUM_CMP` / `__SET_CMP` function.
///
/// Numeric literals and parameters always do (their type decides ordinal vs
/// string at eval time). A **string literal** additionally routes through it
/// for the ordering operators (`<`,`<=`,`>`,`>=`): MySQL orders ENUM/SET by
/// declaration index, not by the string's collation order, so
/// `enum_col < 'medium'` must compare positions — the plain-string comparison
/// the parser would otherwise use is wrong here. Equality (`=`,`<=>`,`<>`)
/// against a string literal is left as a plain, index-friendly string compare
/// (correct because equal string ⟺ equal index for valid members).
pub(super) fn enum_operand_shape(expr: &Expr, op: BinaryOp) -> bool {
    cmp_operand_shape(expr)
        || matches!(
            expr,
            Expr::Identifier(_)
                | Expr::CompoundIdentifier(_)
                | Expr::BoundValue(_)
                | Expr::UserVariable(_)
                | Expr::SystemVariable { .. }
                | Expr::GroupKeyRef { .. }
                | Expr::Function { .. }
                | Expr::Binary { .. }
                | Expr::Unary { .. }
        )
        || (matches!(
            op,
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq
        ) && matches!(expr, Expr::Literal(crate::parser::Literal::String(_))))
}

/// Apply the same query-block numeric semantics to DML before compilation or
/// row substitution loses the ENUM/SET column's declared member list.
pub(crate) fn rewrite_enum_set_dml(db: &Database, statement: &mut crate::parser::Statement) {
    use crate::parser::Statement;
    let mut columns = EnumSetColumns::default();
    match statement {
        Statement::Update(stmt) => {
            columns.add_table(db, &stmt.table, stmt.primary_alias.as_deref());
            for join in &stmt.joins {
                columns.add_table(db, &join.table.name, join.table.alias.as_deref());
            }
        }
        Statement::Delete(stmt) => {
            columns.add_table(db, &stmt.table, stmt.primary_alias.as_deref());
            for join in &stmt.joins {
                columns.add_table(db, &join.table.name, join.table.alias.as_deref());
            }
        }
        Statement::Insert(stmt) => columns.add_table(db, &stmt.table, None),
        _ => return,
    }
    if columns.is_empty() {
        return;
    }
    let rewrite = |expr: &mut Expr| {
        rewrite_enum_set_function_numeric_args(expr, &columns);
        rewrite_enum_set_comparisons(expr, &columns);
    };
    match statement {
        Statement::Update(stmt) => {
            for assignment in &mut stmt.assignments {
                rewrite(&mut assignment.value);
            }
            if let Some(expr) = &mut stmt.selection {
                rewrite(expr);
            }
            for join in &mut stmt.joins {
                if let Some(expr) = &mut join.condition {
                    rewrite(expr);
                }
            }
        }
        Statement::Delete(stmt) => {
            if let Some(expr) = &mut stmt.selection {
                rewrite(expr);
            }
            for join in &mut stmt.joins {
                if let Some(expr) = &mut join.condition {
                    rewrite(expr);
                }
            }
        }
        Statement::Insert(stmt) => {
            if let Some(assignments) = &mut stmt.on_duplicate_key_update {
                for assignment in assignments {
                    rewrite(&mut assignment.value);
                }
            }
        }
        _ => {}
    }
}
