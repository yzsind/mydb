//! Statement-stable identity reads; unknown sibling effects still prevent
//! speculative evaluation because the scheduling contract stays ordered.
use super::*;

pub(super) struct SessionRead {
    pub(super) value: Value,
}
impl SessionRead {
    pub(super) fn bind(node: &ExprNode) -> Option<Self> {
        let ExprNode::Function { kernel, args, .. } = node else {
            return None;
        };
        if matches!(kernel, FunctionKernel::LegacyDeterministic("CAST")) {
            let [source @ ExprNode::Function {
                kernel: FunctionKernel::RlsContext,
                ..
            }, ExprNode::Const(Value::String(target))] = args.as_slice()
            else {
                return None;
            };
            if !matches!(
                target.trim().to_ascii_uppercase().as_str(),
                "SIGNED" | "SIGNED INTEGER" | "UNSIGNED" | "UNSIGNED INTEGER"
            ) {
                return None;
            }
            let ExprNode::Function {
                args: context_args, ..
            } = source
            else {
                return None;
            };
            let [ExprNode::Const(Value::String(key))] = context_args.as_slice() else {
                return None;
            };
            let unsigned = target.trim().to_ascii_uppercase().starts_with("UNSIGNED");
            // A private execution binding, not a constant in a shared plan.
            // Invalid inputs retain the row oracle and its warning ordering.
            let value = crate::engine::rls::context_integer_slot(key, unsigned, || {
                let input = Self::bind(source)?.value;
                let expression = Expr::Function {
                    name: "CAST".into(),
                    distinct: false,
                    args: vec![
                        Expr::BoundValue(input),
                        Expr::Literal(crate::parser::Literal::String(target.clone())),
                    ],
                };
                let diagnostics = crate::engine::api::StatementWarningSink::default();
                let value =
                    crate::engine::api::with_statement_warning_sink(diagnostics.clone(), || {
                        crate::engine::expr::eval_const_runtime(&expression)
                    })
                    .ok()?;
                if !diagnostics.lock().is_empty() {
                    return None;
                }
                Some(value)
            })?;
            return Some(Self { value });
        }
        if matches!(kernel, FunctionKernel::RlsContext) {
            if let [ExprNode::Const(Value::String(key))] = args.as_slice() {
                return Some(Self {
                    value: crate::engine::expr::row_security_context(key),
                });
            }
            return None;
        }
        if !args.is_empty() {
            return None;
        }
        let value = match kernel {
            FunctionKernel::Database => crate::engine::expr::eval_database_func(),
            FunctionKernel::Version => crate::engine::expr::eval_version_func(),
            FunctionKernel::User => crate::engine::expr::eval_user_session_func(),
            FunctionKernel::CurrentUser => crate::engine::expr::eval_current_user_func(),
            FunctionKernel::ConnectionId => crate::engine::expr::eval_connection_id_func(),
            _ => return None,
        };
        Some(Self { value })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: match self.value {
                Value::Null => Domain::Null,
                Value::Int(value) => Domain::Integer(value, value),
                Value::UInt(..) => Domain::Unsigned,
                _ => Domain::Text,
            },
            parallel: false,
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        let bytes = self
            .value
            .allocated_bytes()
            .checked_add(32)?
            .checked_mul(batch.num_rows())?
            .checked_add(512)?;
        Some((bytes, bytes.checked_mul(2)?))
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        evaluate(&ExprNode::Const(self.value.clone()), batch)
    }
}
