/// Durable membership expression for a partial index. Runtime programs are
/// rebuilt against the owning table, never serialized as a Rust AST.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexPredicate {
    pub expr_sql: String,
    pub referenced_columns: Vec<String>,
    pub settings: crate::engine::expr::PolicyExpressionSettings,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_zone: Option<String>,
    #[serde(skip)]
    pub(crate) ast: Option<Expr>,
    #[serde(skip)]
    pub(crate) program: Option<crate::engine::expr_program::CompiledTableExpr>,
}
impl IndexPredicate {
    pub(crate) fn new(expr_sql: String) -> Arc<Self> {
        Arc::new(Self {
            expr_sql,
            referenced_columns: Vec::new(),
            settings: crate::engine::expr::capture_index_settings(),
            system_zone: crate::engine::expr::persistent_index_system_zone().map(str::to_string),
            ast: None,
            program: None,
        })
    }
}
impl TableSchema {
    pub(crate) fn compile_index_predicates(&mut self) -> Result<(), QueryError> {
        if !self.indexes.iter().any(|i| i.predicate.is_some()) {
            return Ok(());
        }
        let compiled = self
            .indexes
            .iter()
            .map(|index| {
                let Some(predicate) = index.predicate.as_deref() else {
                    return Ok(None);
                };
                let mut predicate = predicate.clone();
                if !crate::engine::expr::index_settings_have_portable_zone(&predicate.settings) {
                    return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "Partial index predicates require a resolvable SYSTEM time zone; set time_zone to an explicit IANA zone or UTC offset"));
                }
                if index.primary {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "PRIMARY KEY cannot have a WHERE predicate",
                    ));
                }
                let mut ast =
                    crate::parser::parse_catalog_expr(&predicate.expr_sql).map_err(|e| {
                        error(
                            crate::error::catalog::ER_PARSE_ERROR,
                            format!("Invalid partial index predicate: {e}"),
                        )
                    })?;
                validate_functional_index_expression(&ast, &predicate.expr_sql)?;
                if expr_contains_function_named(&ast, "CONVERT_TZ") {
                    if predicate.system_zone.is_none() {
                        return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "CONVERT_TZ in a partial index requires a resolvable SYSTEM time zone"));
                    }
                } else { predicate.system_zone = None; }
                crate::parser::visit::try_for_each_expr(&ast, |node| {
                    if let Expr::Function { name, .. } = node {
                        let upper = name.to_ascii_uppercase();
                        let volatile = crate::engine::expr_type::builtin_spec(&upper)
                            .is_some_and(|spec| spec.volatility == crate::engine::expr_type::Volatility::Volatile);
                        if volatile || upper.starts_with("AI_") || upper.starts_with("KEYRING_")
                            || upper.starts_with("__MYDB_RLS_")
                            || matches!(upper.as_str(), "RLS_CONTEXT" | "ICU_VERSION" | "__MATCH_AGAINST") {
                            return Err(error(crate::error::catalog::ER_FUNCTIONAL_INDEX_FUNCTION_IS_NOT_ALLOWED,
                                format!("Partial index predicate uses non-deterministic function {name}()")));
                        }
                    }
                    if let Expr::CompoundIdentifier(parts) = node {
                        let local = match parts.as_slice() {
                            [owner, _] => owner.eq_ignore_ascii_case(&self.name),
                            [db, owner, _] => {
                                db.eq_ignore_ascii_case(&self.database)
                                    && owner.eq_ignore_ascii_case(&self.name)
                            }
                            _ => false,
                        };
                        if !local {
                            return Err(error(
                                crate::error::catalog::ER_BAD_FIELD_ERROR,
                                "Partial index predicate cannot reference another table",
                            ));
                        }
                    }
                    Ok(true)
                })?;
                struct LocalColumns;
                impl crate::parser::visit::AstVisitorMut for LocalColumns {
                    fn enter_expr(&mut self, expr: &mut Expr) {
                        if let Expr::CompoundIdentifier(parts) = expr {
                            *expr = Expr::Identifier(
                                parts.last().expect("validated local column").clone(),
                            );
                        }
                    }
                }
                crate::parser::visit::visit_expr_mut(&mut LocalColumns, &mut ast);
                predicate.expr_sql = crate::parser::index_expression_to_catalog_sql(&ast, false)
                    .map_err(|e| error(crate::error::catalog::ER_PARSE_ERROR, e.to_string()))?;
                predicate.referenced_columns = collect_index_expression_columns(&ast, self)?;
                predicate.program = Some(crate::engine::expr::with_index_system_zone(predicate.system_zone.as_deref(), ||
                    crate::engine::expr::with_index_settings(&predicate.settings,
                        || crate::engine::expr_program::CompiledTableExpr::compile_once(&ast, self)))?);
                predicate.ast = Some(ast);
                Ok(Some(Arc::new(predicate)))
            })
            .collect::<Result<Vec<_>, QueryError>>()?;
        for (index, predicate) in self.indexes.iter_mut().zip(compiled) {
            index.predicate = predicate;
        }
        Ok(())
    }
    #[inline]
    pub(crate) fn index_contains_row(
        &self,
        index: &IndexSchema,
        values: &[Value],
    ) -> Result<bool, QueryError> {
        let Some(predicate) = &index.predicate else {
            return Ok(true);
        };
        crate::engine::expr::with_index_system_zone(predicate.system_zone.as_deref(), || {
            crate::engine::expr::with_index_settings(&predicate.settings, || {
                let value = match &predicate.program {
                    Some(program) => program.eval(self, values)?,
                    None => {
                        let ast = predicate.ast.clone().map(Ok).unwrap_or_else(|| {
                            crate::parser::parse_catalog_expr(&predicate.expr_sql).map_err(|e| {
                                error(
                                    crate::error::catalog::ER_PARSE_ERROR,
                                    format!("Invalid partial index predicate: {e}"),
                                )
                            })
                        })?;
                        crate::engine::expr::eval_expr_legacy(&ast, self, values)?
                    }
                };
                Ok(crate::engine::expr::truthy(&value))
            })
        })?
    }
}

thread_local! {
    static PARTIAL_INDEX_PROOFS: std::cell::RefCell<Vec<(u64, Vec<String>)>> = const { std::cell::RefCell::new(Vec::new()) };
}
/// Query-local proof scope. Callers without a proof can never accidentally
/// treat a partial tree as a complete table, including metadata optimizations.
pub(crate) struct PartialIndexScope;
impl PartialIndexScope {
    pub(crate) fn new(table: &TableSchema, predicate: Option<&Expr>, alias: Option<&str>) -> Self {
        if !table.indexes.iter().any(|i| i.predicate.is_some()) {
            PARTIAL_INDEX_PROOFS.with(|s| s.borrow_mut().push((table.id, Vec::new())));
            return Self;
        }
        let params = crate::engine::params::snapshot_current_params()
            .or_else(crate::planner::partial_index_planning_params)
            .unwrap_or_default();
        let settings = crate::engine::expr::capture_index_settings();
        let mut nodes = 0usize;
        if predicate.is_some_and(|p| {
            crate::parser::visit::try_for_each_expr(p, |_| {
                nodes += 1;
                if nodes > 1024 {
                    Err(())
                } else {
                    Ok(true)
                }
            })
            .is_err()
        }) {
            PARTIAL_INDEX_PROOFS.with(|s| s.borrow_mut().push((table.id, Vec::new())));
            return Self;
        }
        let normalize = |expr: &Expr| {
            use crate::parser::visit::{visit_expr_mut, AstVisitorMut};
            struct Normalize<'a> {
                table: &'a str,
                alias: Option<&'a str>,
                params: &'a [Value],
            }
            impl AstVisitorMut for Normalize<'_> {
                fn enter_expr(&mut self, expr: &mut Expr) {
                    use crate::parser::Literal as L;
                    match expr {
                        Expr::Identifier(name) => *name = name.to_ascii_lowercase(),
                        Expr::CompoundIdentifier(parts)
                            if parts.len() == 2
                                && (parts[0].eq_ignore_ascii_case(self.table)
                                    || self
                                        .alias
                                        .is_some_and(|a| parts[0].eq_ignore_ascii_case(a))) =>
                        {
                            *expr = Expr::Identifier(parts[1].to_ascii_lowercase());
                        }
                        Expr::Function { name, .. } => *name = name.to_ascii_uppercase(),
                        Expr::Placeholder(i) => {
                            if let Some(v) = self.params.get(*i) {
                                *expr = Expr::BoundValue(v.clone());
                            }
                        }
                        _ => {}
                    }
                    if let Expr::BoundValue(value) = expr {
                        let literal = match value {
                            Value::Int(v) => Some(L::Int(*v)),
                            Value::UInt(v) => Some(L::UInt(*v)),
                            Value::Null => Some(L::Null),
                            Value::String(v) => Some(L::String(v.clone())),
                            Value::SmallString(v) => std::str::from_utf8(v)
                                .ok()
                                .map(|s| L::String(s.to_string())),
                            _ => None,
                        };
                        if let Some(l) = literal {
                            *expr = Expr::Literal(l);
                        }
                    }
                }
            }
            let mut result = expr.clone();
            visit_expr_mut(
                &mut Normalize {
                    table: &table.name,
                    alias,
                    params: &params,
                },
                &mut result,
            );
            result
        };
        let query = predicate
            .map(normalize)
            .unwrap_or_else(|| Expr::Literal(crate::parser::Literal::Bool(true)));
        let names = table
            .indexes
            .iter()
            .filter_map(|index| {
                let condition = index.predicate.as_ref()?;
                if condition.settings != settings
                    || condition.system_zone.as_deref().is_some_and(|zone| {
                        Some(zone) != crate::engine::expr::persistent_index_system_zone()
                    })
                {
                    return None;
                }
                let ast = normalize(condition.ast.as_ref()?);
                predicate_implies(table, &query, &ast, &mut 512).then(|| index.name.clone())
            })
            .collect();
        PARTIAL_INDEX_PROOFS.with(|s| s.borrow_mut().push((table.id, names)));
        Self
    }
}
impl Drop for PartialIndexScope {
    fn drop(&mut self) {
        PARTIAL_INDEX_PROOFS.with(|s| {
            s.borrow_mut().pop();
        });
    }
}
fn partial_index_queryable(table: &TableSchema, index: &IndexSchema) -> bool {
    index.predicate.is_none()
        || PARTIAL_INDEX_PROOFS.with(|s| {
            s.borrow()
                .last()
                .is_some_and(|(id, names)| *id == table.id && names.contains(&index.name))
        })
}

/// Conservative implication under SQL TRUE semantics. Unsupported expressions
/// can match structurally, but never participate in algebraic approximations.
fn predicate_implies(table: &TableSchema, q: &Expr, p: &Expr, budget: &mut usize) -> bool {
    use crate::parser::{BinaryOp as B, Literal as L};
    if *budget == 0 {
        return false;
    }
    *budget -= 1;
    if q == p {
        return true;
    }
    // Normalize only the top-level atom. Rewriting NULL-safe comparisons
    // underneath NOT would change UNKNOWN into FALSE and is unsound.
    fn equality_atom(e: &Expr) -> Option<Expr> {
        let Expr::Binary {
            left,
            op: B::NullSafeEq,
            right,
        } = e
        else {
            return None;
        };
        if matches!(right.as_ref(), Expr::Literal(L::Null)) {
            return Some(Expr::IsNull(left.clone()));
        }
        if matches!(left.as_ref(), Expr::Literal(L::Null)) {
            return Some(Expr::IsNull(right.clone()));
        }
        if matches!(right.as_ref(), Expr::Literal(_)) || matches!(left.as_ref(), Expr::Literal(_)) {
            return Some(Expr::Binary {
                left: left.clone(),
                op: B::Eq,
                right: right.clone(),
            });
        }
        None
    }
    if let Some(atom) = equality_atom(q) {
        return predicate_implies(table, &atom, p, budget);
    }
    if let Some(atom) = equality_atom(p) {
        return predicate_implies(table, q, &atom, budget);
    }
    fn reversed(e: &Expr) -> Option<Expr> {
        let Expr::Binary { left, op, right } = e else {
            return None;
        };
        if !matches!(left.as_ref(), Expr::Literal(_) | Expr::Unary { .. })
            || !matches!(right.as_ref(), Expr::Identifier(_))
        {
            return None;
        }
        let op = match op {
            B::Eq => B::Eq,
            B::NotEq => B::NotEq,
            B::Lt => B::Gt,
            B::LtEq => B::GtEq,
            B::Gt => B::Lt,
            B::GtEq => B::LtEq,
            _ => return None,
        };
        Some(Expr::Binary {
            left: right.clone(),
            op,
            right: left.clone(),
        })
    }
    if let Some(atom) = reversed(q) {
        return predicate_implies(table, &atom, p, budget);
    }
    if let Some(atom) = reversed(p) {
        return predicate_implies(table, q, &atom, budget);
    }
    match (q, p) {
        (
            Expr::Binary {
                left,
                op: B::Or,
                right,
            },
            _,
        ) => {
            return predicate_implies(table, left, p, budget)
                && predicate_implies(table, right, p, budget)
        }
        (
            _,
            Expr::Binary {
                left,
                op: B::And,
                right,
            },
        ) => {
            return predicate_implies(table, q, left, budget)
                && predicate_implies(table, q, right, budget)
        }
        (
            Expr::Binary {
                left,
                op: B::And,
                right,
            },
            _,
        ) => {
            if predicate_implies(table, left, p, budget)
                || predicate_implies(table, right, p, budget)
            {
                return true;
            }
        }
        (
            _,
            Expr::Binary {
                left,
                op: B::Or,
                right,
            },
        ) => {
            return predicate_implies(table, q, left, budget)
                || predicate_implies(table, q, right, budget)
        }
        (_, Expr::Literal(L::Bool(true) | L::Int(1))) => return true,
        (
            Expr::Between {
                expr,
                low,
                high,
                negated: false,
            },
            _,
        ) => {
            let a = Expr::Binary {
                left: expr.clone(),
                op: B::GtEq,
                right: low.clone(),
            };
            let b = Expr::Binary {
                left: expr.clone(),
                op: B::LtEq,
                right: high.clone(),
            };
            return predicate_implies(table, &a, p, budget)
                || predicate_implies(table, &b, p, budget);
        }
        (
            Expr::InList {
                expr,
                list,
                negated: false,
            },
            _,
        ) => {
            return !list.is_empty()
                && list.iter().all(|v| {
                    matches!(v, Expr::Literal(L::Null))
                        || predicate_implies(
                            table,
                            &Expr::Binary {
                                left: expr.clone(),
                                op: B::Eq,
                                right: Box::new(v.clone()),
                            },
                            p,
                            budget,
                        )
                })
        }
        (
            _,
            Expr::InList {
                expr,
                list,
                negated: false,
            },
        ) => {
            return list.iter().any(|v| {
                predicate_implies(
                    table,
                    q,
                    &Expr::Binary {
                        left: expr.clone(),
                        op: B::Eq,
                        right: Box::new(v.clone()),
                    },
                    budget,
                )
            })
        }
        (
            _,
            Expr::Between {
                expr,
                low,
                high,
                negated: false,
            },
        ) => {
            return predicate_implies(
                table,
                q,
                &Expr::Binary {
                    left: expr.clone(),
                    op: B::GtEq,
                    right: low.clone(),
                },
                budget,
            ) && predicate_implies(
                table,
                q,
                &Expr::Binary {
                    left: expr.clone(),
                    op: B::LtEq,
                    right: high.clone(),
                },
                budget,
            )
        }
        _ => {}
    }
    fn col(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Identifier(n) => Some(n),
            _ => None,
        }
    }
    fn integer(expr: &Expr) -> Option<i128> {
        match expr {
            Expr::Literal(L::Int(v)) => Some(*v as i128),
            Expr::Literal(L::UInt(v)) => Some(*v as i128),
            Expr::Unary {
                op: crate::parser::UnaryOp::Neg,
                expr,
            } => integer(expr).and_then(i128::checked_neg),
            _ => None,
        }
    }
    if let Expr::IsNotNull(target) = p {
        if let Expr::Binary { left, op, right } = q {
            if matches!(op, B::Eq | B::NotEq | B::Gt | B::GtEq | B::Lt | B::LtEq)
                && (left == target || right == target)
            {
                return true;
            }
        }
    }
    let (
        Expr::Binary {
            left: ql,
            op: qo,
            right: qr,
        },
        Expr::Binary {
            left: pl,
            op: po,
            right: pr,
        },
    ) = (q, p)
    else {
        return false;
    };
    let (Some(qc), Some(pc)) = (col(ql), col(pl)) else {
        return false;
    };
    if !qc.eq_ignore_ascii_case(pc) {
        return false;
    }
    let Ok(ci) = table.column_index(qc) else {
        return false;
    };
    // Integer comparisons are exact for every signed/unsigned integer width.
    if !matches!(
        table.columns[ci].data_type,
        DataTypeKind::TinyInt
            | DataTypeKind::SmallInt
            | DataTypeKind::MediumInt
            | DataTypeKind::Int
            | DataTypeKind::BigInt
    ) {
        return false;
    }
    let (Some(a), Some(b)) = (integer(qr), integer(pr)) else {
        return false;
    };
    match (qo, po) {
        (B::Eq, B::Eq) => a == b,
        (B::Eq, B::NotEq) => a != b,
        (B::Eq, B::Gt) => a > b,
        (B::Eq, B::GtEq) => a >= b,
        (B::Eq, B::Lt) => a < b,
        (B::Eq, B::LtEq) => a <= b,
        (B::Gt, B::Gt | B::GtEq) | (B::GtEq, B::GtEq) => a >= b,
        (B::GtEq, B::Gt) => a > b,
        (B::Lt, B::Lt | B::LtEq) | (B::LtEq, B::LtEq) => a <= b,
        (B::LtEq, B::Lt) => a < b,
        _ => false,
    }
}
