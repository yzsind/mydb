//! Exact numeric and integer formatting kernels. Wide coefficients and
//! diagnostic-producing conversions retain the ordered scalar evaluator.
use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Floor,
    Ceil,
    Round,
    Truncate,
    Radix(u8),
    Cast,
    BitCount,
    Mod(i64),
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Input {
    Signed,
    Unsigned,
    Decimal,
}
pub(super) struct NumericRead {
    column: usize,
    kind: Kind,
    input: Input,
    places: i32,
    dynamic_scale: bool,
}
fn places_arg(node: &ExprNode) -> Option<i32> {
    let parse = |value: &Value| match value {
        Value::Int(n) => Some(crate::engine::expr::clamp_round_decimals(*n)),
        Value::UInt(n) => Some((*n).min(i32::MAX as u64) as i32),
        _ => None,
    };
    match node {
        ExprNode::Const(value) => parse(value),
        ExprNode::Placeholder(index) => {
            crate::engine::params::with_current_param(*index, |value| parse(value?))
        }
        _ => None,
    }
}
fn signed_arg(node: &ExprNode) -> Option<i64> {
    let parse = |value: &Value| {
        if let Value::Int(n) = value {
            Some(*n)
        } else {
            None
        }
    };
    match node {
        ExprNode::Const(value) => parse(value),
        ExprNode::Placeholder(index) => {
            crate::engine::params::with_current_param(*index, |value| parse(value?))
        }
        _ => None,
    }
}
impl NumericRead {
    pub(super) fn bind(node: &ExprNode, schema: &BatchSchema) -> Option<Self> {
        let ExprNode::Function { kernel, args, .. } = node else {
            return None;
        };
        let (kind, places, dynamic_scale) = match kernel {
            FunctionKernel::Floor if args.len() == 1 => (Kind::Floor, 0, false),
            FunctionKernel::Ceil if args.len() == 1 => (Kind::Ceil, 0, false),
            FunctionKernel::Round if (1..=2).contains(&args.len()) => {
                let places = match args.get(1) {
                    None => 0,
                    Some(arg) => places_arg(arg)?,
                };
                (
                    Kind::Round,
                    places,
                    args.get(1)
                        .is_some_and(|a| !matches!(a, ExprNode::Const(_))),
                )
            }
            FunctionKernel::Truncate if args.len() == 2 => {
                let places = places_arg(&args[1])?;
                (Kind::Truncate, places, false)
            }
            FunctionKernel::Hex if args.len() == 1 => (Kind::Radix(16), 0, false),
            FunctionKernel::Mod if args.len() == 2 => {
                let n = signed_arg(&args[1])?;
                if n == 0 {
                    return None;
                }
                (Kind::Mod(n), 0, false)
            }
            FunctionKernel::LegacyDeterministic("BIN") if args.len() == 1 => {
                (Kind::Radix(2), 0, false)
            }
            FunctionKernel::LegacyDeterministic("OCT") if args.len() == 1 => {
                (Kind::Radix(8), 0, false)
            }
            FunctionKernel::LegacyDeterministic("BIT_COUNT") if args.len() == 1 => {
                (Kind::BitCount, 0, false)
            }
            FunctionKernel::LegacyDeterministic("CAST")
                if args.len() == 2 && text(&args[1])?.eq_ignore_ascii_case("CHAR") =>
            {
                (Kind::Cast, 0, false)
            }
            _ => return None,
        };
        let ExprNode::Column(column) = args.first()? else {
            return None;
        };
        let field = schema.fields().get(*column)?;
        if field.source.flags
            & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
            != 0
        {
            return None;
        }
        let input = match field.column_type {
            ColumnType::Tiny
            | ColumnType::Short
            | ColumnType::MediumInt
            | ColumnType::Long
            | ColumnType::LongLong => {
                if field.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0 {
                    Input::Unsigned
                } else {
                    Input::Signed
                }
            }
            ColumnType::Decimal { precision, scale }
                if matches!(
                    kind,
                    Kind::Floor | Kind::Ceil | Kind::Round | Kind::Truncate
                ) =>
            {
                if matches!(kind, Kind::Floor | Kind::Ceil)
                    && (precision < scale || precision - scale + u8::from(scale > 0) > 18)
                {
                    return None;
                }
                Input::Decimal
            }
            _ => return None,
        };
        // Keep the existing signed integer identity kernels unchanged.
        if input == Input::Signed && matches!(kind, Kind::Floor | Kind::Ceil | Kind::Cast) {
            return None;
        }
        if input == Input::Decimal
            && matches!(
                kind,
                Kind::Radix(_) | Kind::Cast | Kind::BitCount | Kind::Mod(_)
            )
        {
            return None;
        }
        if input != Input::Signed && matches!(kind, Kind::Mod(_)) {
            return None;
        }
        Some(Self {
            column: *column,
            kind,
            input,
            places,
            dynamic_scale,
        })
    }
    pub(super) fn contract(&self) -> Contract {
        let domain = match self.kind {
            Kind::Radix(_) | Kind::Cast => Domain::Text,
            Kind::BitCount => Domain::Integer(0, 64),
            Kind::Round | Kind::Truncate if self.input == Input::Decimal => Domain::Exact,
            _ if self.input == Input::Unsigned => Domain::Unsigned,
            _ => Domain::Integer(i64::MIN, i64::MAX),
        };
        Contract {
            domain,
            parallel: matches!(self.kind, Kind::Radix(_) | Kind::Cast | Kind::BitCount)
                || self.input == Input::Unsigned && matches!(self.kind, Kind::Floor | Kind::Ceil)
                || matches!(self.kind,Kind::Mod(n) if n != -1),
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let width = match self.kind {
            Kind::Radix(2) => 64,
            Kind::Radix(8) => 22,
            Kind::Radix(16) => 16,
            Kind::Cast => 20,
            _ => 32,
        };
        let payload = batch
            .num_rows()
            .checked_mul(width)?
            .checked_add(batch.num_rows().checked_add(1)?.checked_mul(8)?)?;
        Some((
            payload,
            payload
                .checked_mul(2)?
                .checked_add(batch.num_rows().checked_mul(32)?)?
                .checked_add(512)?,
        ))
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let input = batch.columns().get(self.column)?;
        let rows = batch.num_rows();
        if matches!(input, ColumnVector::Null { .. }) {
            return Some(ColumnVector::Null { len: rows });
        }
        match (self.input, input) {
            (Input::Signed, ColumnVector::I64 { .. })
            | (Input::Unsigned, ColumnVector::U64 { .. })
            | (
                Input::Decimal,
                ColumnVector::Decimal32 { .. }
                | ColumnVector::Decimal64 { .. }
                | ColumnVector::Decimal128 { .. },
            ) => {}
            _ => return None,
        }
        let scale = match input {
            ColumnVector::Decimal32 { scale, .. }
            | ColumnVector::Decimal64 { scale, .. }
            | ColumnVector::Decimal128 { scale, .. } => *scale,
            _ => 0,
        };
        let decimal =
            self.input == Input::Decimal && matches!(self.kind, Kind::Round | Kind::Truncate);
        let text = matches!(self.kind, Kind::Radix(_) | Kind::Cast);
        let mut exact = Vec::with_capacity(if decimal { rows } else { 0 });
        let mut signed = Vec::with_capacity(
            if !decimal && !text && self.input != Input::Unsigned || self.kind == Kind::BitCount {
                rows
            } else {
                0
            },
        );
        let mut unsigned = Vec::with_capacity(
            if !decimal && !text && self.input == Input::Unsigned && self.kind != Kind::BitCount {
                rows
            } else {
                0
            },
        );
        let mut data = Vec::new();
        let mut offsets = if text {
            Vec::with_capacity(rows + 1)
        } else {
            Vec::new()
        };
        if text {
            offsets.push(0);
        }
        let mut output_scale = if decimal && !self.dynamic_scale {
            if self.kind == Kind::Round {
                crate::util::decimal::round_exact_parts(0, scale, self.places)?.1
            } else {
                crate::util::decimal::truncate_exact_parts(0, scale, self.places)?.1
            }
        } else {
            scale
        };
        let divisor =
            if self.input == Input::Decimal && matches!(self.kind, Kind::Floor | Kind::Ceil) {
                crate::util::decimal::pow10_i128(u32::from(scale))?
            } else {
                1
            };
        for row in 0..rows {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            if input.is_null(row) {
                if decimal {
                    exact.push(0);
                } else if text {
                    offsets.push(u32::try_from(data.len()).ok()?);
                } else if self.input == Input::Unsigned && self.kind != Kind::BitCount {
                    unsigned.push(0);
                } else {
                    signed.push(0);
                }
                continue;
            }
            let magnitude = match input {
                ColumnVector::I64 { data, .. } => i128::from(data[row]),
                ColumnVector::U64 { data, .. } => i128::from(data[row]),
                ColumnVector::Decimal32 { data, .. } => i128::from(data[row]),
                ColumnVector::Decimal64 { data, .. } => i128::from(data[row]),
                ColumnVector::Decimal128 { data, .. } => data[row],
                _ => return None,
            };
            match self.kind {
                Kind::Radix(base) => {
                    let mut value = magnitude as u64;
                    let mut buffer = [0u8; 64];
                    let mut pos = buffer.len();
                    let bits = match base {
                        2 => 1,
                        8 => 3,
                        16 => 4,
                        _ => return None,
                    };
                    let mask = (1u64 << bits) - 1;
                    loop {
                        pos -= 1;
                        buffer[pos] = b"0123456789ABCDEF"[(value & mask) as usize];
                        value >>= bits;
                        if value == 0 {
                            break;
                        }
                    }
                    data.extend_from_slice(&buffer[pos..]);
                    offsets.push(u32::try_from(data.len()).ok()?);
                }
                Kind::Cast => {
                    let mut buffer = itoa::Buffer::new();
                    data.extend_from_slice(buffer.format(magnitude as u64).as_bytes());
                    offsets.push(u32::try_from(data.len()).ok()?);
                }
                Kind::BitCount => signed.push(i64::from((magnitude as u64).count_ones())),
                Kind::Mod(n) => signed.push(i64::try_from(magnitude).ok()?.checked_rem(n)?),
                Kind::Floor | Kind::Ceil => {
                    if self.input == Input::Unsigned {
                        unsigned.push(magnitude as u64);
                    } else {
                        let q = magnitude.div_euclid(divisor);
                        let q = q.checked_add(i128::from(
                            self.kind == Kind::Ceil && magnitude.rem_euclid(divisor) != 0,
                        ))?;
                        signed.push(i64::try_from(q).ok()?);
                    }
                }
                Kind::Round | Kind::Truncate => {
                    let (mut value, mut result_scale) = if self.kind == Kind::Round {
                        crate::util::decimal::round_exact_parts(magnitude, scale, self.places)?
                    } else {
                        crate::util::decimal::truncate_exact_parts(magnitude, scale, self.places)?
                    };
                    if decimal && self.dynamic_scale {
                        value =
                            crate::util::decimal::rescale_trunc_i128(value, result_scale, scale)?;
                        result_scale = scale;
                    }
                    if decimal {
                        exact.push(value);
                        output_scale = result_scale;
                    } else if self.input == Input::Unsigned {
                        unsigned.push(u64::try_from(value).ok()?);
                    } else {
                        signed.push(i64::try_from(value).ok()?);
                    }
                }
            }
        }
        let nulls = super::validity(input);
        Some(if text {
            ColumnVector::Utf8 {
                data,
                offsets,
                nulls,
            }
        } else if decimal {
            ColumnVector::Decimal128 {
                data: exact,
                scale: output_scale,
                nulls,
            }
        } else if self.input == Input::Unsigned && self.kind != Kind::BitCount {
            ColumnVector::U64 {
                data: unsigned,
                nulls,
            }
        } else {
            ColumnVector::I64 {
                data: signed,
                nulls,
            }
        })
    }
}
