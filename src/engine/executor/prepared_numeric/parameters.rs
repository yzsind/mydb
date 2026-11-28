use super::*;
use crate::engine::api::PreparedNumericType;

/// Only enable fixed bindings when every parameter has a proven context.
/// Unmodelled function/assignment domains retain the existing executor.
pub(super) fn derive(
    statement: &Statement,
    db: &Database,
) -> Option<Vec<Option<PreparedNumericType>>> {
    let columns = dispatch::collect_statement_table_uses(statement)
        .into_iter()
        .filter_map(|usage| {
            db.table(&usage.table)
                .ok()
                .map(|table| (table, usage.alias))
        })
        .collect::<Vec<_>>();
    struct Bind<'a> {
        types: Vec<Option<PreparedNumericType>>,
        columns: &'a [(Arc<TableSchema>, Option<String>)],
        parameters: std::collections::HashSet<usize>,
        pinned: std::collections::HashSet<usize>,
        top_level: bool,
    }
    impl Bind<'_> {
        fn source(&self, expr: &Expr) -> Option<PreparedNumericType> {
            struct HasParam(bool);
            impl crate::parser::visit::AstVisitor for HasParam {
                fn enter_expr(&mut self, expr: &Expr) {
                    self.0 |= matches!(expr, Expr::Placeholder(_));
                }
            }
            let mut has_param = HasParam(false);
            crate::parser::visit::visit_expr(&mut has_param, expr);
            if has_param.0 {
                return None;
            }
            let column = |expr: &Expr| {
                let (name, qualifier, database) = match expr {
                    Expr::Identifier(name) => (name.as_str(), None, None),
                    Expr::CompoundIdentifier(parts) => (
                        parts.last()?.as_str(),
                        parts.get(parts.len().checked_sub(2)?).map(String::as_str),
                        parts
                            .len()
                            .checked_sub(3)
                            .and_then(|i| parts.get(i))
                            .map(String::as_str),
                    ),
                    _ => return None,
                };
                let mut candidates = self
                    .columns
                    .iter()
                    .filter(|(table, alias)| {
                        qualifier.is_none_or(|q| {
                            table.name.eq_ignore_ascii_case(q)
                                || alias.as_deref().is_some_and(|a| a.eq_ignore_ascii_case(q))
                        }) && database.is_none_or(|d| table.database.eq_ignore_ascii_case(d))
                    })
                    .filter_map(|(table, _)| {
                        table.column_index(name).ok().map(|index| (table, index))
                    });
                let (table, index) = candidates.next()?;
                if candidates.next().is_some() {
                    return None;
                }
                let col = &table.columns[index];
                let source = table.wire_column_source(index);
                Some(crate::engine::expr_type::ExprType::from_data_type(
                    &col.data_type,
                    col.unsigned,
                    col.nullable,
                    source.length,
                    source.collation_id,
                ))
            };
            let no_param = |_: usize| None;
            let context = crate::engine::expr_type::TypeContext::context_free(&column, &no_param);
            use crate::engine::expr_type::ExprTypeKind;
            match crate::engine::expr_type::infer_expr_type(expr, &context).kind {
                ExprTypeKind::Integer { unsigned: true, .. } => Some(PreparedNumericType::Unsigned),
                ExprTypeKind::Integer { .. } => Some(PreparedNumericType::Signed),
                ExprTypeKind::Decimal { .. } => Some(PreparedNumericType::Decimal),
                ExprTypeKind::Float { .. } => Some(PreparedNumericType::Double),
                ExprTypeKind::String { .. } => Some(PreparedNumericType::String),
                _ => None,
            }
        }
        fn assign(&mut self, expr: &Expr, domain: PreparedNumericType, overwrite: bool) {
            match expr {
                Expr::Placeholder(index) => {
                    self.types.resize(self.types.len().max(index + 1), None);
                    if overwrite || self.types[*index].is_none() {
                        self.types[*index] = Some(domain);
                    }
                }
                Expr::Unary {
                    op: UnaryOp::Neg,
                    expr,
                } => self.assign(
                    expr,
                    if domain == PreparedNumericType::String {
                        PreparedNumericType::Double
                    } else {
                        domain
                    },
                    overwrite,
                ),
                _ => {}
            }
        }
    }
    impl crate::parser::visit::AstVisitor for Bind<'_> {
        fn enter_select(&mut self, select: &Select) {
            if self.top_level {
                for item in &select.projection {
                    if let SelectItem::Expr {
                        expr: Expr::Placeholder(index),
                        ..
                    } = item
                    {
                        self.assign(
                            &Expr::Placeholder(*index),
                            PreparedNumericType::String,
                            false,
                        );
                    }
                }
            }
        }
        fn enter_expr(&mut self, expr: &Expr) {
            if let Expr::Placeholder(index) = expr {
                self.parameters.insert(*index);
            }
            if let Expr::Unary {
                op: UnaryOp::Neg,
                expr,
            } = expr
            {
                self.assign(expr, PreparedNumericType::Double, false);
            }
            if let Expr::Function { name, args, .. } = expr {
                if name.eq_ignore_ascii_case("CAST") {
                    if let Some(Expr::Placeholder(index)) = args.first() {
                        self.pinned.insert(*index);
                    }
                }
                // These functions bind an otherwise untyped numeric argument
                // as DOUBLE. Other function signatures require their own proof.
                if ["AVG", "SUM", "ABS"]
                    .iter()
                    .any(|n| name.eq_ignore_ascii_case(n))
                {
                    for arg in args {
                        self.assign(arg, PreparedNumericType::Double, false);
                    }
                }
            }
            if let Expr::Binary { left, op, right } = expr {
                let arithmetic = matches!(
                    op,
                    BinaryOp::Add
                        | BinaryOp::Sub
                        | BinaryOp::Mul
                        | BinaryOp::Div
                        | BinaryOp::IntDiv
                        | BinaryOp::Mod
                );
                let comparison = matches!(
                    op,
                    BinaryOp::Eq
                        | BinaryOp::NullSafeEq
                        | BinaryOp::NotEq
                        | BinaryOp::Lt
                        | BinaryOp::LtEq
                        | BinaryOp::Gt
                        | BinaryOp::GtEq
                );
                if arithmetic || comparison {
                    let promote = |domain| {
                        if arithmetic && domain == PreparedNumericType::String {
                            PreparedNumericType::Double
                        } else {
                            domain
                        }
                    };
                    if let Some(domain) = self.source(right) {
                        self.assign(left, promote(domain), true);
                    }
                    if let Some(domain) = self.source(left) {
                        self.assign(right, promote(domain), true);
                    }
                    if matches!(left.as_ref(), Expr::Placeholder(_))
                        && matches!(right.as_ref(), Expr::Placeholder(_))
                    {
                        let domain = if arithmetic {
                            PreparedNumericType::Double
                        } else {
                            PreparedNumericType::String
                        };
                        self.assign(left, domain, true);
                        self.assign(right, domain, true);
                    }
                }
            }
        }
    }
    let mut bind = Bind {
        types: Vec::new(),
        columns: &columns,
        parameters: Default::default(),
        pinned: Default::default(),
        top_level: !matches!(statement, Statement::Insert(_)),
    };
    crate::parser::visit::visit_statement(&mut bind, statement);
    if bind.parameters.iter().any(|index| {
        !bind.pinned.contains(index) && bind.types.get(*index).is_none_or(Option::is_none)
    }) {
        None
    } else {
        Some(bind.types)
    }
}

fn actual(value: &Value, domain: PreparedNumericType) -> Option<PreparedNumericType> {
    match value {
        Value::Int(_) => Some(PreparedNumericType::Signed),
        Value::UInt(_) => Some(PreparedNumericType::Unsigned),
        Value::DecimalCompact(..)
        | Value::Decimal128(..)
        | Value::DecimalWide(..)
        | Value::Decimal(_) => Some(PreparedNumericType::Decimal),
        Value::Float(_) | Value::Double(_) => Some(PreparedNumericType::Double),
        Value::String(_) | Value::SmallString(_) => {
            if domain == PreparedNumericType::String {
                return Some(PreparedNumericType::String);
            }
            let text = crate::types::value_str(value)?.trim();
            if text.is_empty() {
                return None;
            }
            let parsed = crate::util::decimal::parse_lenient_f64(text);
            if !parsed.clean || !parsed.value.is_finite() {
                return None;
            }
            if text.contains(['e', 'E']) {
                Some(PreparedNumericType::Double)
            } else if text.contains('.') {
                Some(PreparedNumericType::Decimal)
            } else if text.parse::<i64>().is_ok() {
                Some(PreparedNumericType::Signed)
            } else if text.parse::<u64>().is_ok() {
                Some(PreparedNumericType::Unsigned)
            } else {
                Some(PreparedNumericType::Decimal)
            }
        }
        _ => None,
    }
}

impl PreparedNumericContext {
    /// MySQL keeps derived numeric domains until an incompatible value
    /// forces reprepare. Clean conversions are effect-free; malformed text
    /// stays with the ordered expression evaluator and its diagnostics.
    pub(crate) fn bind_parameters(
        &mut self,
        session: &SessionContext,
        values: &mut [Value],
    ) -> Result<(), QueryError> {
        let Some(parameters) = self.parameters.as_ref() else {
            return Ok(());
        };
        let mut next = None;
        for (index, domain) in parameters.types.iter().enumerate() {
            let (Some(domain), Some(value)) = (domain, values.get(index)) else {
                continue;
            };
            let Some(value_type) = actual(value, *domain) else {
                continue;
            };
            let compatible = *domain == value_type
                || *domain == PreparedNumericType::Double
                || (*domain == PreparedNumericType::Decimal
                    && matches!(
                        value_type,
                        PreparedNumericType::Signed | PreparedNumericType::Unsigned
                    ));
            if !compatible {
                next.get_or_insert_with(|| parameters.types.to_vec())[index] = Some(value_type);
            }
        }
        if let Some(next) = next {
            let charge = mem_budget::ControlledMemoryCharge::try_new(
                mem_budget::ControlledMemoryCategory::DerivedCache,
                next.len() * std::mem::size_of::<Option<PreparedNumericType>>()
                    + std::mem::size_of::<crate::engine::api::PreparedNumericParameters>()
                    + 32,
            )?;
            self.parameters = Some(Arc::new(crate::engine::api::PreparedNumericParameters {
                types: next.into_boxed_slice(),
                _memory_charge: charge,
            }));
            self.precision = session.sysvars.div_precision_increment.min(30) as u8;
            self.parameter_reprepared = true;
        }
        for (index, domain) in self
            .parameters
            .as_ref()
            .expect("retained parameter binding")
            .types
            .iter()
            .enumerate()
        {
            let (Some(domain), Some(value)) = (domain, values.get_mut(index)) else {
                continue;
            };
            if matches!(value, Value::Null) {
                continue;
            }
            if *domain == PreparedNumericType::Double {
                if let Some(text) = crate::types::value_str(value) {
                    let parsed = crate::util::decimal::parse_lenient_f64(text);
                    if !text.trim().is_empty() && parsed.clean && parsed.value.is_finite() {
                        *value = Value::Double(parsed.value);
                    }
                } else if actual(value, *domain).is_some() {
                    *value = Value::Double(crate::engine::expr::numeric(value)?);
                }
            } else if let Some(text) = crate::types::value_str(value) {
                let text = text.trim();
                match domain {
                    PreparedNumericType::Signed => {
                        if let Ok(v) = text.parse::<i64>() {
                            *value = Value::Int(v);
                        }
                    }
                    PreparedNumericType::Unsigned => {
                        if let Ok(v) = text.parse::<u64>() {
                            *value = Value::UInt(v);
                        }
                    }
                    PreparedNumericType::Decimal => {
                        if let Some(v) = crate::engine::expr::exact_numeric_string_operand(value) {
                            *value = v;
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
}
