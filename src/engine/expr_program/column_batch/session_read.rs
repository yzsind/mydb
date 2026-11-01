//! Statement-stable identity reads; unknown sibling effects still prevent
//! speculative evaluation because the scheduling contract stays ordered.
use super::*;

pub(super) struct SessionRead {
    value: Value,
}
impl SessionRead {
    pub(super) fn bind(node: &ExprNode) -> Option<Self> {
        let ExprNode::Function { kernel, args, .. } = node else {
            return None;
        };
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
                Value::Int(..) | Value::UInt(..) => Domain::Unsigned,
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
