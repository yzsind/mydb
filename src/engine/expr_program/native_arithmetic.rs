//! Compact, effect-free arithmetic tapes. A refusal leaves the original tree
//! responsible for coercion, NULL, overflow, division by zero and diagnostics.
use super::*;

const STACK_SLOTS: usize = 16;
const MAX_INSTRUCTIONS: usize = 127;

#[derive(Debug, Clone, Copy)]
enum Instruction {
    IntegerColumn(usize),
    ExactColumn(usize),
    IntegerParameter(usize),
    ExactParameter(usize),
    Constant(i64),
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    AddConstant(i64),
    SubtractConstant(i64),
    MultiplyConstant(i64),
    DivideConstant(i64),
    RemainderConstant(i64),
}

#[derive(Debug)]
pub(super) struct NativeArithmetic {
    instructions: Box<[Instruction]>,
    decimal: bool,
}

#[derive(Default)]
struct Builder {
    instructions: Vec<Instruction>,
    depth: usize,
    operations: usize,
    decimal: bool,
}

impl Builder {
    fn emit(&mut self, node: &ExprNode, exact: bool) -> Option<()> {
        let leaf = match node {
            ExprNode::Column(index) => {
                if exact {
                    Instruction::ExactColumn(*index)
                } else {
                    Instruction::IntegerColumn(*index)
                }
            }
            ExprNode::Placeholder(index) => {
                if exact {
                    Instruction::ExactParameter(*index)
                } else {
                    Instruction::IntegerParameter(*index)
                }
            }
            ExprNode::Const(Value::Int(integer)) => Instruction::Constant(*integer),
            ExprNode::Const(Value::DecimalCompact(integer, 0)) if exact => {
                self.decimal = true;
                Instruction::Constant(*integer)
            }
            ExprNode::Const(Value::Decimal128(integer, 0)) if exact => {
                self.decimal = true;
                Instruction::Constant(i64::try_from(*integer).ok()?)
            }
            ExprNode::Binary {
                left,
                op,
                right,
                semantics,
            } => {
                let exact = if semantics.checked_integer_tree() {
                    false
                } else if semantics.checked_zero_scale_tree() {
                    true
                } else {
                    return None;
                };
                self.emit(left, exact)?;
                self.emit(right, exact)?;
                return self.binary(*op);
            }
            _ => return None,
        };
        if self.depth == STACK_SLOTS || self.instructions.len() == MAX_INSTRUCTIONS {
            return None;
        }
        self.instructions.push(leaf);
        self.depth += 1;
        Some(())
    }

    fn binary(&mut self, op: BinaryOp) -> Option<()> {
        use Instruction::*;
        self.decimal |= op == BinaryOp::Div;
        let constant = match self.instructions.last() {
            Some(Constant(value)) => Some(*value),
            _ => None,
        };
        let instruction = match (op, constant) {
            (BinaryOp::Add, Some(value)) => AddConstant(value),
            (BinaryOp::Sub, Some(value)) => SubtractConstant(value),
            (BinaryOp::Mul, Some(value)) => MultiplyConstant(value),
            (BinaryOp::Div | BinaryOp::IntDiv, Some(value)) => DivideConstant(value),
            (BinaryOp::Mod, Some(value)) => RemainderConstant(value),
            (BinaryOp::Add, None) => Add,
            (BinaryOp::Sub, None) => Subtract,
            (BinaryOp::Mul, None) => Multiply,
            (BinaryOp::Div | BinaryOp::IntDiv, None) => Divide,
            (BinaryOp::Mod, None) => Remainder,
            _ => return None,
        };
        if constant.is_some() {
            self.instructions.pop();
        }
        if self.instructions.len() == MAX_INSTRUCTIONS {
            return None;
        }
        self.instructions.push(instruction);
        self.depth -= 1;
        self.operations += 1;
        Some(())
    }
}

impl NativeArithmetic {
    pub(super) fn compile(
        left: &mut ExprNode,
        op: BinaryOp,
        right: &mut ExprNode,
        kernel: IntegerKernel,
    ) -> Option<Arc<Self>> {
        let exact = match kernel {
            IntegerKernel::CheckedArithmetic => false,
            IntegerKernel::CheckedZeroScaleArithmetic => true,
            _ => return None,
        };
        let mut builder = Builder::default();
        builder.emit(left, exact)?;
        builder.emit(right, exact)?;
        builder.binary(op)?;
        debug_assert_eq!(builder.depth, 1);
        // One operation already has a small direct evaluator. Parent tapes
        // replace child tapes without retaining redundant instruction storage.
        if builder.operations < 2 {
            return None;
        }
        Self::clear(left);
        Self::clear(right);
        Some(Arc::new(Self {
            instructions: builder.instructions.into_boxed_slice(),
            decimal: builder.decimal,
        }))
    }

    fn clear(node: &mut ExprNode) {
        if let ExprNode::Binary {
            left,
            right,
            semantics,
            ..
        } = node
        {
            semantics.native_arithmetic = None;
            Self::clear(left);
            Self::clear(right);
        }
    }

    pub(super) fn heap_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.instructions.len() * std::mem::size_of::<Instruction>()
            + 16
    }

    #[inline]
    fn integer(value: &Value) -> Option<i64> {
        match value {
            Value::Int(value) => Some(*value),
            _ => None,
        }
    }

    #[inline]
    fn exact(value: &Value, decimal: &mut bool) -> Option<i64> {
        let integer = match value {
            Value::Int(value) => return Some(*value),
            Value::DecimalCompact(value, 0) => *value,
            Value::Decimal128(value, 0) => i64::try_from(*value).ok()?,
            _ => return None,
        };
        *decimal = true;
        Some(integer)
    }

    pub(super) fn eval(&self, row: &dyn RowSource) -> Option<Value> {
        use Instruction::*;
        let mut stack = [0i64; STACK_SLOTS];
        let mut depth = 0;
        // Exact arithmetic propagates a DECIMAL lane through every admitted
        // parent. Track it once; pure integer subtrees still require Int leaves.
        let mut decimal = self.decimal;
        macro_rules! binary {
            ($method:ident) => {{
                let right = stack[depth - 1];
                depth -= 1;
                stack[depth - 1] = stack[depth - 1].$method(right)?;
                continue;
            }};
        }
        macro_rules! immediate {
            ($method:ident, $right:expr) => {{
                stack[depth - 1] = stack[depth - 1].$method($right)?;
                continue;
            }};
        }
        for instruction in self.instructions.iter() {
            let integer = match *instruction {
                IntegerColumn(index) => Self::integer(row.value_ref(index)?)?,
                ExactColumn(index) => Self::exact(row.value_ref(index)?, &mut decimal)?,
                IntegerParameter(index) => {
                    crate::engine::params::with_current_param(index, |value| Self::integer(value?))?
                }
                ExactParameter(index) => {
                    crate::engine::params::with_current_param(index, |value| {
                        Self::exact(value?, &mut decimal)
                    })?
                }
                Constant(integer) => integer,
                Add => binary!(checked_add),
                Subtract => binary!(checked_sub),
                Multiply => binary!(checked_mul),
                Divide => binary!(checked_div),
                Remainder => binary!(checked_rem),
                AddConstant(right) => immediate!(checked_add, right),
                SubtractConstant(right) => immediate!(checked_sub, right),
                MultiplyConstant(right) => immediate!(checked_mul, right),
                DivideConstant(right) => immediate!(checked_div, right),
                RemainderConstant(right) => immediate!(checked_rem, right),
            };
            stack[depth] = integer;
            depth += 1;
        }
        Some(if decimal {
            Value::DecimalCompact(stack[0], 0)
        } else {
            Value::Int(stack[0])
        })
    }
}
