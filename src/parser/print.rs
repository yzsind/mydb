//! SQL presentation and literal encoding, independent of AST conversion.

use super::{current_parser_sql_mode_bits, BinaryOp, Expr, Literal, SystemVariableScope, UnaryOp};

pub(super) fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Function names lose their source quoting in the scalar AST. Restore
/// quoting for punctuation and syntax keywords while retaining native-call
/// spelling (quoting a native name would mean a stored function in MySQL).
fn function_name_sql(name: &str) -> std::borrow::Cow<'_, str> {
    fn needs_quotes(part: &str) -> bool {
        if part.is_empty()
            || part.as_bytes()[0].is_ascii_digit()
            || !part
                .chars()
                .all(|ch| ch.is_alphanumeric() || matches!(ch, '_' | '$'))
        {
            return true;
        }
        let upper = if part.bytes().any(|byte| byte.is_ascii_lowercase()) {
            std::borrow::Cow::Owned(part.to_ascii_uppercase())
        } else {
            std::borrow::Cow::Borrowed(part)
        };
        sqlparser::keywords::ALL_KEYWORDS
            .binary_search(&upper.as_ref())
            .is_ok()
            && !crate::engine::cbo::function_cost::is_known_builtin_function(part)
    }
    if !name.split('.').any(needs_quotes) {
        return std::borrow::Cow::Borrowed(name);
    }
    std::borrow::Cow::Owned(
        name.split('.')
            .map(|part| {
                if needs_quotes(part) {
                    quote_ident(part)
                } else {
                    part.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("."),
    )
}

/// Public wrapper over [`literal_to_sql`] for the view-body normalizer.
pub fn literal_to_sql_pub(lit: &Literal) -> String {
    literal_to_sql(lit)
}

/// Public wrapper over [`binary_op_sql`] for the view-body normalizer.
pub fn binary_op_sql_pub(op: BinaryOp) -> &'static str {
    binary_op_sql(op)
}

pub(super) fn literal_to_sql(lit: &Literal) -> String {
    match lit {
        Literal::Null => "NULL".to_string(),
        Literal::Bool(value) => if *value { "TRUE" } else { "FALSE" }.to_string(),
        Literal::Int(value) => value.to_string(),
        // Small decimal tokens parse as signed integers. Hex retains unsigned
        // provenance in the numeric contexts that create small UInt literals.
        Literal::UInt(value) if *value <= i64::MAX as u64 => format!("0x{value:X}"),
        Literal::UInt(value) => value.to_string(),
        Literal::Float(value) => {
            // An exponent preserves approximate-number type even when the
            // shortest spelling has no decimal point (1.0 must not become 1).
            let mut out = value.to_string();
            if !out.contains(['e', 'E']) {
                out.push_str("e0");
            }
            out
        }
        Literal::Decimal(value) => value.clone(),
        Literal::String(value) => {
            let escaped = if current_parser_sql_mode_bits()
                & crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES
                == 0
            {
                value
                    .replace('\\', "\\\\")
                    .replace('\0', "\\0")
                    .replace('\n', "\\n")
                    .replace('\r', "\\r")
                    .replace('\t', "\\t")
                    .replace('\u{8}', "\\b")
                    .replace('\u{1a}', "\\Z")
            } else {
                value.clone()
            };
            format!("'{}'", escaped.replace('\'', "''"))
        }
        Literal::HexBytes(b) => {
            if b.is_empty() {
                return "X''".to_string();
            }
            let mut s = String::with_capacity(2 + 2 * b.len());
            s.push_str("0x");
            crate::util::hex::append_upper(&mut s, b);
            s
        }
        Literal::BitBytes(b) => {
            // Re-render as b'…' bit string. Each byte is 8 bits MSB-first.
            let mut s = String::with_capacity(3 + 8 * b.len());
            s.push_str("b'");
            for byte in b {
                for i in (0..8).rev() {
                    s.push(if (byte >> i) & 1 == 1 { '1' } else { '0' });
                }
            }
            s.push('\'');
            s
        }
    }
}

pub(super) fn binary_op_sql(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Or => "OR",
        BinaryOp::And => "AND",
        BinaryOp::Xor => "XOR",
        BinaryOp::Eq => "=",
        BinaryOp::NullSafeEq => "<=>",
        BinaryOp::NotEq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::LtEq => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::GtEq => ">=",
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::IntDiv => "DIV",
        BinaryOp::Mod => "%",
        BinaryOp::BitAnd => "&",
        BinaryOp::BitOr => "|",
        BinaryOp::BitXor => "^",
        BinaryOp::ShiftLeft => "<<",
        BinaryOp::ShiftRight => ">>",
        BinaryOp::Arrow => "->",
        BinaryOp::LongArrow => "->>",
        BinaryOp::Concat => "||",
    }
}

/// The expression cannot be faithfully represented by the SQL encoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlEncodeError(&'static str);

impl std::fmt::Display for SqlEncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cannot encode SQL expression: {}", self.0)
    }
}
impl std::error::Error for SqlEncodeError {}

/// Encode supported scalar syntax without display-only fallbacks. Bound
/// execution nodes, query expressions, and malformed nodes fail explicitly.
/// Lexical escaping follows the active parser mode; catalog callers fix it.
pub fn try_expr_to_sql(expr: &Expr) -> Result<String, SqlEncodeError> {
    validate_scalar_encoding(expr)?;
    Ok(render_expr(expr, true))
}

fn validate_scalar_encoding(expr: &Expr) -> Result<(), SqlEncodeError> {
    use super::visit::{visit_expr, AstVisitor};
    struct Validate {
        depth: usize,
        numeric_contexts: Vec<bool>,
        error: Option<SqlEncodeError>,
    }
    impl AstVisitor for Validate {
        fn enter_expr(&mut self, expr: &Expr) {
            self.depth += 1;
            let numeric_parent = self.numeric_contexts.last().copied().unwrap_or(false);
            self.numeric_contexts
                .push(super::convert::has_numeric_literal_children(expr));
            if self.error.is_some() {
                return;
            }
            let reason = if self.depth > crate::engine::expr_program::MAX_EXPRESSION_DEPTH {
                Some("expression depth exceeds the supported limit")
            } else {
                match expr {
                    Expr::Subquery(_)
                    | Expr::InSubquery { .. }
                    | Expr::Exists { .. }
                    | Expr::QuantifiedComparison { .. } => {
                        Some("query expression is not supported by the scalar encoder")
                    }
                    Expr::WindowFunction { .. } => {
                        Some("window expression is not supported by the scalar encoder")
                    }
                    Expr::BoundValue(_) | Expr::GroupKeyRef { .. } | Expr::Grouping(_) => {
                        Some("bound execution node has no source SQL representation")
                    }
                    Expr::Placeholder(_) => {
                        Some("parameter identity cannot be persisted as scalar SQL")
                    }
                    Expr::Case {
                        conditions,
                        results,
                        ..
                    } if conditions.is_empty() || conditions.len() != results.len() => {
                        Some("CASE conditions and results do not match")
                    }
                    Expr::Literal(Literal::UInt(value))
                        if *value <= i64::MAX as u64 && !numeric_parent =>
                    {
                        Some("small unsigned literal requires a numeric SQL context")
                    }
                    Expr::Literal(Literal::Float(value)) if !value.is_finite() => {
                        Some("non-finite floating point literal")
                    }
                    Expr::CompoundIdentifier(parts) if parts.is_empty() => {
                        Some("empty qualified identifier")
                    }
                    Expr::Tuple(items) if items.len() < 2 => {
                        Some("ROW requires at least two expressions")
                    }
                    Expr::Wildcard
                    | Expr::Identifier(_)
                    | Expr::CompoundIdentifier(_)
                    | Expr::UserVariable(_)
                    | Expr::SystemVariable { .. }
                    | Expr::Literal(_)
                    | Expr::Default(_)
                    | Expr::DefaultQualified(_)
                    | Expr::Unary { .. }
                    | Expr::Binary { .. }
                    | Expr::IsNull(_)
                    | Expr::IsNotNull(_)
                    | Expr::InList { .. }
                    | Expr::Between { .. }
                    | Expr::Like { .. }
                    | Expr::Case { .. }
                    | Expr::Function { .. }
                    | Expr::Tuple(_) => None,
                }
            };
            self.error = reason.map(SqlEncodeError);
        }
        fn exit_expr(&mut self, _: &Expr) {
            self.depth -= 1;
            self.numeric_contexts.pop();
        }
        fn walk_expr_children(&self, _: &Expr) -> bool {
            self.error.is_none()
        }
    }
    let mut validator = Validate {
        depth: 0,
        numeric_contexts: Vec::new(),
        error: None,
    };
    visit_expr(&mut validator, expr);
    if let Some(error) = validator.error {
        return Err(error);
    }
    Ok(())
}

/// Persisted schema expressions use a fixed lexical mode, independent of the
/// creating connection and of the connection that later rebuilds the schema.
pub(crate) fn expr_to_catalog_sql(expr: &Expr) -> Result<String, SqlEncodeError> {
    let _mode = super::ParserSqlModeGuard::activate(0);
    try_expr_to_sql(expr)
}

/// Best-effort expression display. Do not use this for executable or
/// persisted SQL: unsupported query/window nodes use a display placeholder.
pub fn expr_to_sql(expr: &Expr) -> String {
    render_expr(expr, false)
}

fn render_expr(expr: &Expr, canonical: bool) -> String {
    let expr_to_sql = |expr: &Expr| render_expr(expr, canonical);
    match expr {
        Expr::Wildcard => "*".to_string(),
        Expr::Identifier(name) => quote_ident(name),
        Expr::CompoundIdentifier(parts) => parts
            .iter()
            .map(|part| quote_ident(part))
            .collect::<Vec<_>>()
            .join("."),
        Expr::UserVariable(name) => {
            if name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '.'))
            {
                format!("@{name}")
            } else {
                format!("@{}", quote_ident(name))
            }
        }
        Expr::SystemVariable { scope, name } => match scope {
            SystemVariableScope::ImplicitSession => format!("@@{name}"),
            SystemVariableScope::Session => format!("@@session.{name}"),
            SystemVariableScope::Local => format!("@@local.{name}"),
            SystemVariableScope::Global => format!("@@global.{name}"),
        },
        Expr::Literal(lit) => literal_to_sql(lit),
        Expr::BoundValue(value) => value.to_display_string(),
        Expr::Placeholder(_) => "?".to_string(),
        Expr::DefaultQualified(parts) => format!(
            "DEFAULT({})",
            parts
                .iter()
                .map(|part| quote_ident(part))
                .collect::<Vec<_>>()
                .join(".")
        ),
        Expr::Default(Some(name)) => format!("DEFAULT({})", quote_ident(name)),
        Expr::Default(None) => "DEFAULT".to_string(),
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => format!("(NOT ({}))", expr_to_sql(expr)),
            UnaryOp::Neg => format!("-({})", expr_to_sql(expr)),
            UnaryOp::BitNot => format!("~({})", expr_to_sql(expr)),
        },
        Expr::Binary {
            left,
            op: BinaryOp::Concat,
            right,
        } => format!("CONCAT({}, {})", expr_to_sql(left), expr_to_sql(right)),
        Expr::Binary { left, op, right } => format!(
            "({} {} {})",
            expr_to_sql(left),
            binary_op_sql(*op),
            expr_to_sql(right)
        ),
        Expr::IsNull(expr) => format!("({} IS NULL)", expr_to_sql(expr)),
        Expr::IsNotNull(expr) => format!("({} IS NOT NULL)", expr_to_sql(expr)),
        Expr::InList {
            expr,
            list,
            negated,
        } => format!(
            "({} {}IN ({}))",
            expr_to_sql(expr),
            if *negated { "NOT " } else { "" },
            list.iter().map(expr_to_sql).collect::<Vec<_>>().join(", ")
        ),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => format!(
            "({} {}BETWEEN {} AND {})",
            expr_to_sql(expr),
            if *negated { "NOT " } else { "" },
            expr_to_sql(low),
            expr_to_sql(high)
        ),
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
        } => {
            let mut out = format!(
                "({} {}LIKE {}",
                expr_to_sql(expr),
                if *negated { "NOT " } else { "" },
                expr_to_sql(pattern)
            );
            match escape_char {
                Some(ch) => {
                    out.push_str(" ESCAPE ");
                    out.push_str(&literal_to_sql(&Literal::String(ch.to_string())));
                }
                None if current_parser_sql_mode_bits()
                    & crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES
                    == 0 =>
                {
                    out.push_str(" ESCAPE ''");
                }
                None => {}
            }
            out.push(')');
            out
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let mut out = String::from("CASE");
            if let Some(operand) = operand {
                out.push(' ');
                out.push_str(&expr_to_sql(operand));
            }
            for (cond, result) in conditions.iter().zip(results.iter()) {
                out.push_str(" WHEN ");
                out.push_str(&expr_to_sql(cond));
                out.push_str(" THEN ");
                out.push_str(&expr_to_sql(result));
            }
            if let Some(else_result) = else_result {
                out.push_str(" ELSE ");
                out.push_str(&expr_to_sql(else_result));
            }
            out.push_str(" END");
            out
        }
        Expr::Function {
            name,
            args,
            distinct: false,
        } if name == "__AT_TIME_ZONE" && args.len() == 2 => format!(
            "{} AT TIME ZONE {}",
            expr_to_sql(&args[0]),
            expr_to_sql(&args[1])
        ),
        Expr::Function {
            name,
            args,
            distinct: false,
        } if name == "__TIMESTAMP_LITERAL" && args.len() == 1 => {
            format!("TIMESTAMP{}", expr_to_sql(&args[0]))
        }
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            if canonical && !distinct {
                if let Some(sql) = native_scalar_sql(name, args, &expr_to_sql) {
                    return sql;
                }
            }
            format!(
                "{}({}{})",
                function_name_sql(name),
                if *distinct { "DISTINCT " } else { "" },
                args.iter().map(expr_to_sql).collect::<Vec<_>>().join(", ")
            )
        }
        Expr::GroupKeyRef { expr, .. } => expr_to_sql(expr),
        Expr::Grouping(indices) => format!(
            "GROUPING({})",
            indices
                .iter()
                .map(|idx| format!("group_{idx}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Tuple(items) => format!(
            "({})",
            items.iter().map(expr_to_sql).collect::<Vec<_>>().join(", ")
        ),
        Expr::Subquery(_)
        | Expr::QuantifiedComparison { .. }
        | Expr::InSubquery { .. }
        | Expr::Exists { .. }
        | Expr::WindowFunction { .. } => "expr".to_string(),
    }
}

fn cast_sql(args: &[Expr], render: &impl Fn(&Expr) -> String, array: bool) -> Option<String> {
    use std::fmt::Write;
    let Expr::Literal(Literal::String(target)) = args.get(1)? else {
        return None;
    };
    let target = target.to_ascii_uppercase();
    let max_extra = match target.as_str() {
        "CHAR" | "BINARY" | "TIME" | "DATETIME" | "VECTOR" => 1,
        "DECIMAL" => 2,
        "SIGNED" | "UNSIGNED" | "FLOAT" | "DOUBLE" | "DATE" | "YEAR" | "JSON" => 0,
        _ => return None,
    };
    if args.len() > 2 + max_extra {
        return None;
    }
    let mut out = format!("CAST({} AS {target}", render(&args[0]));
    if args.len() > 2 {
        out.push('(');
        for (i, arg) in args[2..].iter().enumerate() {
            let number = match arg {
                Expr::Literal(Literal::Int(n)) if *n >= 0 => *n as u64,
                Expr::Literal(Literal::UInt(n)) => *n,
                _ => return None,
            };
            if i > 0 {
                out.push_str(", ");
            }
            write!(&mut out, "{number}").expect("String formatting");
        }
        out.push(')');
    }
    if array {
        out.push_str(" ARRAY");
    }
    out.push(')');
    Some(out)
}

fn native_scalar_sql(
    name: &str,
    args: &[Expr],
    render: &impl Fn(&Expr) -> String,
) -> Option<String> {
    if name.eq_ignore_ascii_case("CAST") {
        return cast_sql(args, render, false);
    }
    if name.eq_ignore_ascii_case("TRIM") && (2..=3).contains(&args.len()) {
        let direction = if args.len() == 3 {
            let Expr::Literal(Literal::String(direction)) = &args[2] else {
                return None;
            };
            let direction = direction.to_ascii_uppercase();
            if !matches!(direction.as_str(), "LEADING" | "TRAILING" | "BOTH") {
                return None;
            }
            format!("{direction} ")
        } else {
            String::new()
        };
        return Some(format!(
            "TRIM({direction}{} FROM {})",
            render(&args[1]),
            render(&args[0])
        ));
    }
    if args.len() == 2 {
        if let Expr::Literal(Literal::String(value)) = &args[1] {
            if name.eq_ignore_ascii_case("COLLATE") {
                return Some(format!(
                    "({} COLLATE {})",
                    render(&args[0]),
                    quote_ident(value)
                ));
            }
            if name.eq_ignore_ascii_case("CONVERT_USING") {
                return Some(format!(
                    "CONVERT({} USING {})",
                    render(&args[0]),
                    quote_ident(value)
                ));
            }
        }
    }
    None
}

/// Functional-index DDL must retain ARRAY independently of runtime scalar
/// CAST evaluation. Catalog expressions are validated before formatting.
pub(crate) fn index_expression_to_catalog_sql(
    expr: &Expr,
    multi_valued: bool,
) -> Result<String, SqlEncodeError> {
    let _mode = super::ParserSqlModeGuard::activate(0);
    validate_scalar_encoding(expr)?;
    if !multi_valued {
        return Ok(render_expr(expr, true));
    }
    if let Expr::Function {
        name,
        args,
        distinct: false,
    } = expr
    {
        if name.eq_ignore_ascii_case("CAST") {
            return cast_sql(args, &|expr| render_expr(expr, true), true).ok_or(SqlEncodeError(
                "multi-valued index requires a static CAST target",
            ));
        }
    }
    Err(SqlEncodeError(
        "multi-valued index requires a CAST expression",
    ))
}
