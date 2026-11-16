use crate::engine::executor::vec::batch::ColumnVector;
use crate::engine::executor::vec::bitmap::Bitmap;
use crate::engine::executor::vec::expr_batch as eb;
use crate::engine::expr_program::CompiledNumericExpr;
use crate::parser::BinaryOp;
use crate::types::Value;

/// Owned-only operand model. `Const` is the broadcast scalar; lanes
/// always carry an owned `Vec` (we materialise the column on entry
/// instead of juggling borrowed slices through the dispatch tree —
/// the extra `Vec::clone` is amortised over the full batch).
pub(crate) enum Lane {
    I64 {
        data: Vec<i64>,
        scale: u8,
        nulls: Option<Bitmap>,
    },
    I128 {
        data: Vec<i128>,
        scale: u8,
        nulls: Option<Bitmap>,
    },
    Const {
        mag: i128,
        scale: u8,
    },
}

impl Lane {
    fn scale(&self) -> u8 {
        match self {
            Lane::I64 { scale, .. } => *scale,
            Lane::I128 { scale, .. } => *scale,
            Lane::Const { scale, .. } => *scale,
        }
    }
}

pub(super) fn const_from_value(v: &Value) -> Option<(i128, u8)> {
    match v {
        Value::Int(x) => Some((*x as i128, 0)),
        Value::UInt(x) if *x <= i64::MAX as u64 => Some((*x as i128, 0)),
        Value::DecimalCompact(mag, scale) => Some((*mag as i128, *scale)),
        _ => None,
    }
}

fn column_to_lane(col: &ColumnVector) -> Option<Lane> {
    if let Some((data, scale)) = col.as_decimal_i32_slice() {
        // Widen i32 → i64 to keep ops on a single integer width.
        let widened: Vec<i64> = data.iter().map(|x| *x as i64).collect();
        return Some(Lane::I64 {
            data: widened,
            scale,
            nulls: col.nulls().cloned(),
        });
    }
    if let Some((data, scale)) = col.as_decimal_i64_slice() {
        return Some(Lane::I64 {
            data: data.to_vec(),
            scale,
            nulls: col.nulls().cloned(),
        });
    }
    if let Some((data, scale)) = col.as_decimal_i128_slice() {
        return Some(Lane::I128 {
            data: data.to_vec(),
            scale,
            nulls: col.nulls().cloned(),
        });
    }
    None
}

/// Scale-up a constant scalar from `from` to `to`. Returns `None`
/// on overflow or on a scale-down request (which would truncate).
fn scale_up_i128_const(mag: i128, from: u8, to: u8) -> Option<i128> {
    if from == to {
        return Some(mag);
    }
    if from > to {
        return None;
    }
    let diff = (to - from) as u32;
    10i128.checked_pow(diff).and_then(|m| mag.checked_mul(m))
}

/// In-place scale-up of an i64 lane. Any overflow declines the batch
/// kernel so the current batch is evaluated by the scalar decimal oracle.
fn scale_up_i64_inplace(data: &mut [i64], from: u8, to: u8) -> Option<()> {
    if from == to {
        return Some(());
    }
    if from > to {
        return None;
    }
    let diff = (to - from) as u32;
    let m = 10i64.checked_pow(diff)?;
    for v in data.iter_mut() {
        *v = v.checked_mul(m)?;
    }
    Some(())
}

/// Scale an already-wide fixed-point lane without narrowing it through i64.
/// Product expressions commonly widen both operands to i128 before a final
/// addition/subtraction (for example `price * rate - cost * quantity`).  The
/// scalar oracle used to handle that final node row by row even though every
/// intermediate value was already an exact scaled integer.
fn scale_up_i128_inplace(data: &mut [i128], from: u8, to: u8) -> Option<()> {
    if from == to {
        return Some(());
    }
    if from > to {
        return None;
    }
    let multiplier = 10i128.checked_pow((to - from) as u32)?;
    for value in data {
        *value = value.checked_mul(multiplier)?;
    }
    Some(())
}

fn binary_addsub(l: Lane, op: BinaryOp, r: Lane, n_rows: usize) -> Option<Lane> {
    let target_scale = l.scale().max(r.scale());
    match (l, r) {
        (Lane::Const { mag: lm, scale: ls }, Lane::Const { mag: rm, scale: rs }) => {
            let l_al = scale_up_i128_const(lm, ls, target_scale)?;
            let r_al = scale_up_i128_const(rm, rs, target_scale)?;
            let v = if matches!(op, BinaryOp::Add) {
                l_al.checked_add(r_al)?
            } else {
                l_al.checked_sub(r_al)?
            };
            Some(Lane::Const {
                mag: v,
                scale: target_scale,
            })
        }
        (
            Lane::Const { mag: cm, scale: cs },
            Lane::I64 {
                mut data,
                scale,
                nulls,
            },
        ) => {
            scale_up_i64_inplace(&mut data, scale, target_scale)?;
            let cm_aligned = scale_up_i128_const(cm, cs, target_scale)?;
            let cm_i64 = i64::try_from(cm_aligned).ok()?;
            let mut out = Vec::with_capacity(n_rows);
            for value in data {
                out.push(match op {
                    BinaryOp::Sub => cm_i64.checked_sub(value)?,
                    BinaryOp::Add => cm_i64.checked_add(value)?,
                    _ => unreachable!(),
                });
            }
            Some(Lane::I64 {
                data: out,
                scale: target_scale,
                nulls,
            })
        }
        (
            Lane::I64 {
                mut data,
                scale,
                nulls,
            },
            Lane::Const { mag: cm, scale: cs },
        ) => {
            scale_up_i64_inplace(&mut data, scale, target_scale)?;
            let cm_aligned = scale_up_i128_const(cm, cs, target_scale)?;
            let cm_i64 = i64::try_from(cm_aligned).ok()?;
            let mut out = Vec::with_capacity(n_rows);
            for value in data {
                out.push(match op {
                    BinaryOp::Add => value.checked_add(cm_i64)?,
                    BinaryOp::Sub => value.checked_sub(cm_i64)?,
                    _ => unreachable!(),
                });
            }
            Some(Lane::I64 {
                data: out,
                scale: target_scale,
                nulls,
            })
        }
        (
            Lane::I64 {
                data: mut la_data,
                scale: la_scale,
                nulls: la_nulls,
            },
            Lane::I64 {
                data: mut rb_data,
                scale: rb_scale,
                nulls: rb_nulls,
            },
        ) => {
            scale_up_i64_inplace(&mut la_data, la_scale, target_scale)?;
            scale_up_i64_inplace(&mut rb_data, rb_scale, target_scale)?;
            let mut out = vec![0i64; n_rows];
            match op {
                BinaryOp::Add => eb::add_i64_simd(&la_data, &rb_data, &mut out),
                BinaryOp::Sub => eb::sub_i64_simd(&la_data, &rb_data, &mut out),
                _ => unreachable!(),
            }
            // The SIMD kernel wraps on i64 overflow. For fixed-point
            // decimal lanes that is wrong — MySQL DECIMAL widens rather
            // than wrapping. Detect signed overflow per-lane and bail to
            // the row-wise path (which widens to i128 / errors) instead of
            // emitting a wrapped value. The check is only a factor when
            // two near-i64::MAX same-scale decimals combine; the common
            // case pays one branch-free pass.
            let overflowed = match op {
                BinaryOp::Add => {
                    (0..n_rows).any(|i| ((la_data[i] ^ out[i]) & (rb_data[i] ^ out[i])) < 0)
                }
                BinaryOp::Sub => {
                    (0..n_rows).any(|i| ((la_data[i] ^ rb_data[i]) & (la_data[i] ^ out[i])) < 0)
                }
                _ => unreachable!(),
            };
            if overflowed {
                return None;
            }
            let nulls = eb::merge_nulls(la_nulls.as_ref(), rb_nulls.as_ref(), n_rows);
            Some(Lane::I64 {
                data: out,
                scale: target_scale,
                nulls,
            })
        }
        (
            Lane::Const { mag, scale },
            Lane::I128 {
                mut data,
                scale: lane_scale,
                nulls,
            },
        ) => {
            scale_up_i128_inplace(&mut data, lane_scale, target_scale)?;
            let constant = scale_up_i128_const(mag, scale, target_scale)?;
            for value in &mut data {
                *value = match op {
                    BinaryOp::Add => constant.checked_add(*value)?,
                    BinaryOp::Sub => constant.checked_sub(*value)?,
                    _ => unreachable!(),
                };
            }
            Some(Lane::I128 {
                data,
                scale: target_scale,
                nulls,
            })
        }
        (
            Lane::I128 {
                mut data,
                scale: lane_scale,
                nulls,
            },
            Lane::Const { mag, scale },
        ) => {
            scale_up_i128_inplace(&mut data, lane_scale, target_scale)?;
            let constant = scale_up_i128_const(mag, scale, target_scale)?;
            for value in &mut data {
                *value = match op {
                    BinaryOp::Add => value.checked_add(constant)?,
                    BinaryOp::Sub => value.checked_sub(constant)?,
                    _ => unreachable!(),
                };
            }
            Some(Lane::I128 {
                data,
                scale: target_scale,
                nulls,
            })
        }
        (
            Lane::I128 {
                data: mut left,
                scale: left_scale,
                nulls: left_nulls,
            },
            Lane::I128 {
                data: mut right,
                scale: right_scale,
                nulls: right_nulls,
            },
        ) => {
            scale_up_i128_inplace(&mut left, left_scale, target_scale)?;
            scale_up_i128_inplace(&mut right, right_scale, target_scale)?;
            for (left, right) in left.iter_mut().zip(&right) {
                *left = match op {
                    BinaryOp::Add => left.checked_add(*right)?,
                    BinaryOp::Sub => left.checked_sub(*right)?,
                    _ => unreachable!(),
                };
            }
            Some(Lane::I128 {
                data: left,
                scale: target_scale,
                nulls: eb::merge_nulls(left_nulls.as_ref(), right_nulls.as_ref(), n_rows),
            })
        }
        (
            Lane::I64 {
                data: left,
                scale: left_scale,
                nulls: left_nulls,
            },
            Lane::I128 {
                data: mut right,
                scale: right_scale,
                nulls: right_nulls,
            },
        ) => {
            scale_up_i128_inplace(&mut right, right_scale, target_scale)?;
            let multiplier = 10i128.checked_pow((target_scale - left_scale) as u32)?;
            for (right, left) in right.iter_mut().zip(&left) {
                let left = i128::from(*left).checked_mul(multiplier)?;
                *right = match op {
                    BinaryOp::Add => left.checked_add(*right)?,
                    BinaryOp::Sub => left.checked_sub(*right)?,
                    _ => unreachable!(),
                };
            }
            Some(Lane::I128 {
                data: right,
                scale: target_scale,
                nulls: eb::merge_nulls(left_nulls.as_ref(), right_nulls.as_ref(), n_rows),
            })
        }
        (
            Lane::I128 {
                data: mut left,
                scale: left_scale,
                nulls: left_nulls,
            },
            Lane::I64 {
                data: right,
                scale: right_scale,
                nulls: right_nulls,
            },
        ) => {
            scale_up_i128_inplace(&mut left, left_scale, target_scale)?;
            let multiplier = 10i128.checked_pow((target_scale - right_scale) as u32)?;
            for (left, right) in left.iter_mut().zip(&right) {
                let right = i128::from(*right).checked_mul(multiplier)?;
                *left = match op {
                    BinaryOp::Add => left.checked_add(right)?,
                    BinaryOp::Sub => left.checked_sub(right)?,
                    _ => unreachable!(),
                };
            }
            Some(Lane::I128 {
                data: left,
                scale: target_scale,
                nulls: eb::merge_nulls(left_nulls.as_ref(), right_nulls.as_ref(), n_rows),
            })
        }
    }
}

fn binary_mul(l: Lane, r: Lane, n_rows: usize) -> Option<Lane> {
    let s_l = l.scale();
    let s_r = r.scale();
    let out_scale = s_l.checked_add(s_r)?;
    if out_scale > 30 {
        return None;
    }
    match (l, r) {
        (Lane::Const { mag: lm, .. }, Lane::Const { mag: rm, .. }) => Some(Lane::Const {
            mag: lm.checked_mul(rm)?,
            scale: out_scale,
        }),
        // Const × i64 / i64 × Const  →  widen to i128.
        (Lane::Const { mag: cm, .. }, Lane::I64 { data, nulls, .. })
        | (Lane::I64 { data, nulls, .. }, Lane::Const { mag: cm, .. }) => {
            let mut out = Vec::with_capacity(n_rows);
            for value in data {
                out.push(cm.checked_mul(value as i128)?);
            }
            Some(Lane::I128 {
                data: out,
                scale: out_scale,
                nulls,
            })
        }
        // Const × i128 / i128 × Const.
        (Lane::Const { mag: cm, .. }, Lane::I128 { data, nulls, .. })
        | (Lane::I128 { data, nulls, .. }, Lane::Const { mag: cm, .. }) => {
            let mut out = Vec::with_capacity(n_rows);
            for value in data {
                out.push(value.checked_mul(cm)?);
            }
            Some(Lane::I128 {
                data: out,
                scale: out_scale,
                nulls,
            })
        }
        // i64 × i64  →  widen to i128.
        (
            Lane::I64 {
                data: la_data,
                nulls: la_nulls,
                ..
            },
            Lane::I64 {
                data: rb_data,
                nulls: rb_nulls,
                ..
            },
        ) => {
            let mut out = vec![0i128; n_rows];
            eb::mul_i64_widen_to_i128(&la_data, &rb_data, &mut out);
            let nulls = eb::merge_nulls(la_nulls.as_ref(), rb_nulls.as_ref(), n_rows);
            Some(Lane::I128 {
                data: out,
                scale: out_scale,
                nulls,
            })
        }
        // i128 × i64.
        (
            Lane::I128 {
                data: la_data,
                nulls: la_nulls,
                ..
            },
            Lane::I64 {
                data: rb_data,
                nulls: rb_nulls,
                ..
            },
        ) => {
            let mut out = Vec::with_capacity(n_rows);
            for (left, right) in la_data.into_iter().zip(&rb_data) {
                out.push(left.checked_mul(i128::from(*right))?);
            }
            let nulls = eb::merge_nulls(la_nulls.as_ref(), rb_nulls.as_ref(), n_rows);
            Some(Lane::I128 {
                data: out,
                scale: out_scale,
                nulls,
            })
        }
        // i64 × i128 (commutativity — same kernel with swapped args).
        (
            Lane::I64 {
                data: la_data,
                nulls: la_nulls,
                ..
            },
            Lane::I128 {
                data: rb_data,
                nulls: rb_nulls,
                ..
            },
        ) => {
            let mut out = Vec::with_capacity(n_rows);
            for (right, left) in rb_data.into_iter().zip(&la_data) {
                out.push(right.checked_mul(i128::from(*left))?);
            }
            let nulls = eb::merge_nulls(la_nulls.as_ref(), rb_nulls.as_ref(), n_rows);
            Some(Lane::I128 {
                data: out,
                scale: out_scale,
                nulls,
            })
        }
        // i128 × i128: rare (would overflow i128 on full TPC-H
        // magnitudes anyway). Bail to row-mode.
        _ => None,
    }
}

/// Bound retained output and peak scratch for the exact-decimal lane kernel.
/// Each arithmetic node may retain both operands while allocating its output.
/// Constants remain scalars until the final broadcast. Unsupported leaves and
/// operators decline before allocation; overflow still declines at execution.
pub(crate) fn decimal_workspace_bound(
    expr: &CompiledNumericExpr,
    columns: &[ColumnVector],
    rows: usize,
) -> Option<(usize, usize)> {
    let lane = rows
        .checked_mul(std::mem::size_of::<i128>())?
        .checked_add(rows.div_ceil(64).checked_mul(std::mem::size_of::<u64>())?)?
        .checked_add(std::mem::size_of::<ColumnVector>() + 64)?;
    fn visit(
        expr: &CompiledNumericExpr,
        columns: &[ColumnVector],
        lane: usize,
    ) -> Option<(usize, usize)> {
        match expr {
            CompiledNumericExpr::Column(index) => match columns.get(*index)? {
                ColumnVector::Decimal32 { .. }
                | ColumnVector::Decimal64 { .. }
                | ColumnVector::Decimal128 { .. } => Some((lane, lane)),
                _ => None,
            },
            CompiledNumericExpr::Const(value) => const_from_value(value).map(|_| (0, 0)),
            CompiledNumericExpr::Binary { left, op, right }
                if matches!(op, BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul) =>
            {
                let (left_live, left_peak) = visit(left, columns, lane)?;
                let (right_live, right_peak) = visit(right, columns, lane)?;
                let peak = left_peak
                    .max(left_live.checked_add(right_peak)?)
                    .max(left_live.checked_add(right_live)?.checked_add(lane)?);
                Some((lane, peak))
            }
            _ => None,
        }
    }
    let (_, peak) = visit(expr, columns, lane)?;
    Some((lane, peak.max(lane)))
}

pub(crate) fn try_eval_lane(
    expr: &CompiledNumericExpr,
    columns: &[ColumnVector],
    n_rows: usize,
) -> Option<Lane> {
    match expr {
        CompiledNumericExpr::Column(idx) => column_to_lane(&columns[*idx]),
        CompiledNumericExpr::Const(v) => {
            const_from_value(v).map(|(mag, scale)| Lane::Const { mag, scale })
        }
        CompiledNumericExpr::Binary { left, op, right } => {
            let l = try_eval_lane(left, columns, n_rows)?;
            let r = try_eval_lane(right, columns, n_rows)?;
            match op {
                BinaryOp::Add | BinaryOp::Sub => binary_addsub(l, *op, r, n_rows),
                BinaryOp::Mul => binary_mul(l, r, n_rows),
                // Div / Mod would need runtime f64 dispatch + scale
                // semantics — out of scope for the Decimal-only path.
                _ => None,
            }
        }
    }
}

pub(crate) fn lane_into_column(lane: Lane, n_rows: usize) -> ColumnVector {
    match lane {
        Lane::I64 { data, scale, nulls } => {
            debug_assert_eq!(data.len(), n_rows);
            ColumnVector::Decimal64 { data, scale, nulls }
        }
        Lane::I128 { data, scale, nulls } => {
            debug_assert_eq!(data.len(), n_rows);
            ColumnVector::Decimal128 { data, scale, nulls }
        }
        Lane::Const { mag, scale } => {
            // Broadcast — Q1 doesn't produce a const-only NumericExpr
            // at the top level (every aggregate's input references a
            // column), but this keeps the conversion total.
            ColumnVector::Decimal128 {
                data: vec![mag; n_rows],
                scale,
                nulls: None,
            }
        }
    }
}

enum IntegerLane {
    Values {
        data: Vec<i64>,
        nulls: Option<Bitmap>,
    },
    Const(i64),
}

fn integer_lane(
    expr: &CompiledNumericExpr,
    columns: &[ColumnVector],
    n_rows: usize,
) -> Option<IntegerLane> {
    match expr {
        CompiledNumericExpr::Column(index) => {
            let ColumnVector::I64 { data, nulls } = columns.get(*index)? else {
                return None;
            };
            (data.len() == n_rows).then(|| IntegerLane::Values {
                data: data.clone(),
                nulls: nulls.clone(),
            })
        }
        CompiledNumericExpr::Const(value) => match value {
            Value::Int(value) => Some(IntegerLane::Const(*value)),
            Value::UInt(value) => i64::try_from(*value).ok().map(IntegerLane::Const),
            _ => None,
        },
        CompiledNumericExpr::Binary { left, op, right }
            if matches!(op, BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul) =>
        {
            let left = integer_lane(left, columns, n_rows)?;
            let right = integer_lane(right, columns, n_rows)?;
            let apply = |left: i64, right: i64| match op {
                BinaryOp::Add => left.checked_add(right),
                BinaryOp::Sub => left.checked_sub(right),
                BinaryOp::Mul => left.checked_mul(right),
                _ => unreachable!(),
            };
            match (left, right) {
                (IntegerLane::Const(left), IntegerLane::Const(right)) => {
                    apply(left, right).map(IntegerLane::Const)
                }
                (IntegerLane::Values { data, nulls }, IntegerLane::Const(right)) => {
                    Some(IntegerLane::Values {
                        data: data
                            .into_iter()
                            .map(|left| apply(left, right))
                            .collect::<Option<Vec<_>>>()?,
                        nulls,
                    })
                }
                (IntegerLane::Const(left), IntegerLane::Values { data, nulls }) => {
                    Some(IntegerLane::Values {
                        data: data
                            .into_iter()
                            .map(|right| apply(left, right))
                            .collect::<Option<Vec<_>>>()?,
                        nulls,
                    })
                }
                (
                    IntegerLane::Values {
                        data: left,
                        nulls: left_nulls,
                    },
                    IntegerLane::Values {
                        data: right,
                        nulls: right_nulls,
                    },
                ) => Some(IntegerLane::Values {
                    data: left
                        .into_iter()
                        .zip(right)
                        .map(|(left, right)| apply(left, right))
                        .collect::<Option<Vec<_>>>()?,
                    nulls: eb::merge_nulls(left_nulls.as_ref(), right_nulls.as_ref(), n_rows),
                }),
            }
        }
        _ => None,
    }
}

pub(crate) fn try_eval_integer_column(
    expr: &CompiledNumericExpr,
    columns: &[ColumnVector],
    n_rows: usize,
) -> Option<ColumnVector> {
    match integer_lane(expr, columns, n_rows)? {
        IntegerLane::Values { data, nulls } => Some(ColumnVector::I64 { data, nulls }),
        IntegerLane::Const(value) => Some(ColumnVector::I64 {
            data: vec![value; n_rows],
            nulls: None,
        }),
    }
}
