//! Scalar window aggregates with statically dispatched row updates.
use super::{
    eval_expr_against_columns, expr_column_position, frame_position_excluded, WindowFrameRange,
};
use crate::engine::executor::select::aggregate::{
    accumulate_numeric_state, finalize_avg_value, numeric_state_to_value, NumericAggState,
};
use crate::engine::expr::compare_values;
use crate::parser::Expr;
use crate::storage::error;
use crate::types::{Column, QueryError, Value};

pub(super) trait WindowAggregate: Default {
    const COUNT_STAR: bool = false;
    fn update(&mut self, value: Value) -> Result<(), QueryError>;
    fn value(&self) -> Value;
    fn finish(self) -> Value {
        self.value()
    }
}

pub(super) fn prefix_values<A: WindowAggregate>(
    values: impl IntoIterator<Item = Value>,
) -> Result<Vec<Value>, QueryError> {
    let values = values.into_iter();
    let mut result = Vec::with_capacity(values.size_hint().0);
    let mut state = A::default();
    for (position, value) in values.enumerate() {
        if position % 256 == 0 {
            crate::engine::cancel::check_cancelled()?;
        }
        state.update(value)?;
        result.push(state.value());
    }
    Ok(result)
}

#[derive(Default)]
pub(super) struct Count<const STAR: bool>(u64);
impl<const STAR: bool> WindowAggregate for Count<STAR> {
    const COUNT_STAR: bool = STAR;
    #[inline]
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        if STAR || !matches!(value, Value::Null) {
            self.0 += 1;
        }
        Ok(())
    }
    fn value(&self) -> Value {
        Value::Int(self.0 as i64)
    }
}

#[derive(Default)]
pub(super) struct Numeric<const AVERAGE: bool> {
    state: NumericAggState,
    count: u64,
}
impl<const AVERAGE: bool> WindowAggregate for Numeric<AVERAGE> {
    #[inline]
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        if !matches!(value, Value::Null) {
            accumulate_numeric_state(&mut self.state, &value)?;
            self.count += 1;
        }
        Ok(())
    }
    fn value(&self) -> Value {
        if AVERAGE {
            finalize_avg_value(&self.state, self.count)
        } else {
            numeric_state_to_value(&self.state)
        }
    }
}

#[derive(Default)]
pub(super) struct Extremum<const MAXIMUM: bool>(Option<Value>);
impl<const MAXIMUM: bool> WindowAggregate for Extremum<MAXIMUM> {
    #[inline]
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        if !matches!(value, Value::Null)
            && self.0.as_ref().is_none_or(|previous| {
                let order = compare_values(&value, previous).unwrap_or(std::cmp::Ordering::Equal);
                if MAXIMUM {
                    order.is_gt()
                } else {
                    order.is_lt()
                }
            })
        {
            self.0 = Some(value);
        }
        Ok(())
    }
    fn value(&self) -> Value {
        self.0.clone().unwrap_or(Value::Null)
    }
    fn finish(self) -> Value {
        self.0.unwrap_or(Value::Null)
    }
}

/// Bind identifier lookup once; retain the expression evaluator for other
/// expressions and malformed rows so their diagnostics remain unchanged.
struct Argument<'a> {
    expr: &'a Expr,
    columns: &'a [Column],
    position: Option<usize>,
}
impl<'a> Argument<'a> {
    fn new(expr: &'a Expr, columns: &'a [Column]) -> Self {
        Self {
            expr,
            columns,
            position: expr_column_position(expr, columns),
        }
    }
    #[inline]
    fn value(&self, row: &[Value]) -> Result<Value, QueryError> {
        if let Some(value) = self.position.and_then(|position| row.get(position)) {
            Ok(value.clone())
        } else {
            eval_expr_against_columns(self.expr, self.columns, row)
        }
    }
}

/// Row access and frame traversal are independent of the aggregate operation.
/// Each method is monomorphized; no enum or virtual call enters the row loop.
pub(super) struct PartitionInput<'a> {
    pub(super) partition: &'a [usize],
    pub(super) columns: &'a [Column],
    pub(super) rows: &'a [Vec<Value>],
}
impl PartitionInput<'_> {
    pub(super) fn whole<A: WindowAggregate>(&self, arg: &Expr) -> Result<Value, QueryError> {
        let mut state = A::default();
        let argument = Argument::new(arg, self.columns);
        for (pos, &row_idx) in self.partition.iter().enumerate() {
            if pos % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            state.update(argument.value(&self.rows[row_idx])?)?;
        }
        Ok(state.finish())
    }

    pub(super) fn prefix<A: WindowAggregate>(
        &self,
        args: &[Expr],
        ranges: &[WindowFrameRange],
        result: &mut [Value],
    ) -> Result<(), QueryError> {
        let mut state = A::default();
        let argument = args.first().map(|arg| Argument::new(arg, self.columns));
        let mut next = 0;
        for (pos, &row_idx) in self.partition.iter().enumerate() {
            if pos % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            while next <= ranges[pos].end {
                if next % 256 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                let value = if A::COUNT_STAR {
                    Value::Int(1)
                } else {
                    let argument = argument.as_ref().ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_WRONG_ARGUMENTS,
                            "window aggregate requires an argument",
                        )
                    })?;
                    argument.value(&self.rows[self.partition[next]])?
                };
                state.update(value)?;
                next += 1;
            }
            result[row_idx] = state.value();
        }
        Ok(())
    }

    pub(super) fn frames<A: WindowAggregate>(
        &self,
        arg: Option<&Expr>,
        ranges: &[WindowFrameRange],
        result: &mut [Value],
    ) -> Result<(), QueryError> {
        let argument = arg.map(|arg| Argument::new(arg, self.columns));
        for (pos, &row_idx) in self.partition.iter().enumerate() {
            if pos % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let range = ranges[pos];
            if A::COUNT_STAR {
                result[row_idx] = Value::Int(range.included_len() as i64);
                continue;
            }
            let mut state = A::default();
            for frame_pos in range.start..=range.end {
                if frame_pos % 256 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                if frame_position_excluded(&range, frame_pos) {
                    continue;
                }
                let value = argument
                    .as_ref()
                    .expect("window aggregate argument checked")
                    .value(&self.rows[self.partition[frame_pos]])?;
                state.update(value)?;
            }
            result[row_idx] = state.finish();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{BinaryOp, Literal};
    use crate::types::ColumnType;

    #[test]
    fn window_argument_binding_preserves_lookup_expressions_and_errors() {
        let columns = ["v", "V"].map(|name| Column {
            name: name.into(),
            column_type: ColumnType::LongLong,
            ..Column::default()
        });
        for expr in [
            Expr::Identifier("V".into()),
            Expr::CompoundIdentifier(vec!["t".into(), "v".into()]),
            Expr::Identifier("missing".into()),
            Expr::Literal(Literal::Int(42)),
            Expr::Binary {
                left: Box::new(Expr::Identifier("v".into())),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(Literal::Int(1))),
            },
        ] {
            let bound = Argument::new(&expr, &columns);
            for row in [
                vec![Value::Int(-7), Value::Int(100)],
                vec![Value::Null, Value::Int(100)],
            ] {
                let normalize = |result: Result<Value, QueryError>| {
                    result.map_err(|error| (error.code(), error.message().to_owned()))
                };
                assert_eq!(
                    normalize(bound.value(&row)),
                    normalize(eval_expr_against_columns(&expr, &columns, &row))
                );
            }
        }
        // Only the identifier shortcut accepts short rows; the general
        // expression evaluator requires the schema and row widths to agree.
        let expr = Expr::Identifier("v".into());
        let bound_error = Argument::new(&expr, &columns).value(&[]).unwrap_err();
        let original_error = eval_expr_against_columns(&expr, &columns, &[]).unwrap_err();
        assert_eq!(bound_error.code(), original_error.code());
        assert_eq!(bound_error.message(), original_error.message());
    }
}
