//! Exact FORMAT kernels; diagnostic-producing conversions retain row order.
use super::*;

pub(super) struct FormatRead {
    column: usize,
    decimals: u8,
    style: crate::engine::expr::NumberStyle,
}
impl FormatRead {
    pub(super) fn bind(node: &ExprNode, schema: &BatchSchema) -> Option<Self> {
        let ExprNode::Function {
            kernel: FunctionKernel::LegacyDeterministic("FORMAT"),
            args,
            ..
        } = node
        else {
            return None;
        };
        if !(2..=3).contains(&args.len()) {
            return None;
        }
        let ExprNode::Column(column) = &args[0] else {
            return None;
        };
        let field = schema.fields().get(*column)?;
        if field.source.flags
            & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
            != 0
            || !matches!(
                field.column_type,
                ColumnType::Tiny
                    | ColumnType::Short
                    | ColumnType::MediumInt
                    | ColumnType::Long
                    | ColumnType::LongLong
                    | ColumnType::Decimal {
                        precision: 0..=38,
                        ..
                    }
            )
        {
            return None;
        }
        let decimals = integer(&args[1])?.clamp(0, 30) as u8;
        let style = match args.get(2) {
            None => crate::engine::expr::locale_number_style_checked("en_US").0,
            Some(locale) => string_read::Text::node(locale)?.with(|s| {
                let (style, known) = crate::engine::expr::locale_number_style_checked(s);
                known.then_some(style)
            })?,
        };
        Some(Self {
            column: *column,
            decimals,
            style,
        })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: Domain::Text,
            parallel: false,
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        // At most 39 coefficient digits, a sign, a decimal point and
        // grouping separators (four UTF-8 bytes each).
        let bytes = batch.num_rows().checked_mul(128)?.checked_add(8)?;
        Some((bytes, bytes.checked_mul(2)?.checked_add(512)?))
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let input = batch.columns().get(self.column)?;
        let scale = match input {
            ColumnVector::I64 { .. } | ColumnVector::U64 { .. } | ColumnVector::Null { .. } => 0,
            ColumnVector::Decimal32 { scale, .. }
            | ColumnVector::Decimal64 { scale, .. }
            | ColumnVector::Decimal128 { scale, .. } => *scale,
            _ => return None,
        };
        let mut data = Vec::new();
        let mut offsets = Vec::with_capacity(batch.num_rows() + 1);
        offsets.push(0);
        for row in 0..batch.num_rows() {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            if !input.is_null(row) {
                let magnitude = match input {
                    ColumnVector::I64 { data, .. } => i128::from(data[row]),
                    ColumnVector::U64 { data, .. } => i128::from(data[row]),
                    ColumnVector::Decimal32 { data, .. } => i128::from(data[row]),
                    ColumnVector::Decimal64 { data, .. } => i128::from(data[row]),
                    ColumnVector::Decimal128 { data, .. } => data[row],
                    _ => return None,
                };
                let (rounded, rounded_scale) = crate::util::decimal::round_exact_parts(
                    magnitude,
                    scale,
                    i32::from(self.decimals),
                )?;
                let pad = u32::from(self.decimals).checked_sub(u32::from(rounded_scale))?;
                let magnitude = rounded.checked_mul(10i128.checked_pow(pad)?)?;
                self.style.append_exact(magnitude, self.decimals, &mut data);
            }
            offsets.push(u32::try_from(data.len()).ok()?);
        }
        Some(ColumnVector::Utf8 {
            data,
            offsets,
            nulls: validity(input),
        })
    }
}
